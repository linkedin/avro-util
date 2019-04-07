/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.codegen;

import com.linkedin.avro.compatibility.AvroCompatibilityHelper;
import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import com.linkedin.avro.compatibility.SchemaParseResult;
import com.linkedin.avro.util.TemplateUtil;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.specific.FixedSize;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tools.ant.DirectoryScanner;
import org.apache.tools.ant.types.selectors.SelectorUtils;

import java.io.*;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CodeGenerator {
    private final static Logger LOG = LogManager.getLogger(CodeGenerator.class);
    private final static Pattern SCHEMA_REDEFINITION = Pattern.compile("Can't redefine:\\s+([\\w.]+)");
    private final static Pattern UNDEFINED_NAME = Pattern.compile("^\"([\\w.]+)\" is not a defined name\\.");
    private final static Pattern UNDEFINED_NAME_IN_UNION = Pattern.compile("^Undefined name:\\s+\"([\\w.]+)\"");
    private final static String FIXED_SCHEMA_TEMPLATE = TemplateUtil.loadTemplate("FixedSchema.template");
    private final static String FIXED_SCHEMA_NO_NAMESPACE_TEMPLATE = TemplateUtil.loadTemplate("FixedSchemaNoNamespace.template");
    private final static String ENUM_SCHEMA_TEMPLATE = TemplateUtil.loadTemplate("EnumSchema.template");
    private final static String ENUM_SCHEMA_NO_NAMESPACE_TEMPLATE = TemplateUtil.loadTemplate("EnumSchemaNoNamespace.template");

    //either files or folders
    private Set<File> inputs;
    //patterns out of inputs to consider
    private Set<String> includes = new HashSet<>(Collections.singletonList("**/*.avsc"));
    //patterns for determining which schemas (after applying inputs+includes above) are importable into other schemas
    private Set<String> importablePatterns = new HashSet<>(Collections.singletonList("**/*.*"));
    //if true will go fishing on the classpath for generated specific record classes of missing schemas
    private boolean allowClasspathLookup = true;
    //if true, and we find a SpecificFixed or Enum class on the classpath (see allowClasspathLookup above)
    //and this class we find was generated by horrible ancient avro and not made compatible (so we cant get
    //its actual schema from it) we can try and "guess" the schema by the class' contents.
    //this has an interesting side effect of allowing avro schemas to use just any java enum ...
    private boolean allowReverseEngineeringClasspathSchemas = true;
    //top-level schema a.b.c.X must be in a file who's path from root is a/b/c/*.avsc
    private boolean validateSchemaNamespaceVsFilePath = false;
    //top-level schema called a.b.c.X must be in a file called X.avsc
    private boolean validateSchemaNameVsFileName = false;
    //if != null, would post-process the code to make it runtime-compatible with the designated version and onwards
    private AvroVersion minTargetAvroVersion = AvroVersion.AVRO_1_4;
    //output root for java files (or null, in which case no results will be written to disk)
    private File outputFolder;

    public void setInputs(File... inputs) {
        setInputs(new HashSet<>(Arrays.asList(inputs)));
    }

    public void setInputs(Set<File> inputs) {
        this.inputs = inputs;
    }

    public void setIncludes(Set<String> includes) {
        this.includes = includes;
    }

    public void setOutputFolder(File outputFolder) {
        this.outputFolder = outputFolder;
    }

    public void setImportablePatterns(Set<String> importablePatterns) {
        this.importablePatterns = importablePatterns;
    }

    public void setAllowClasspathLookup(boolean allowClasspathLookup) {
        this.allowClasspathLookup = allowClasspathLookup;
    }

    public void setAllowReverseEngineeringClasspathSchemas(boolean allowReverseEngineeringClasspathSchemas) {
        this.allowReverseEngineeringClasspathSchemas = allowReverseEngineeringClasspathSchemas;
    }

    public void setValidateSchemaNamespaceVsFilePath(boolean validateSchemaNamespaceVsFilePath) {
        this.validateSchemaNamespaceVsFilePath = validateSchemaNamespaceVsFilePath;
    }

    public void setValidateSchemaNameVsFileName(boolean validateSchemaNameVsFileName) {
        this.validateSchemaNameVsFileName = validateSchemaNameVsFileName;
    }

    public void setMinTargetAvroVersion(AvroVersion minTargetAvroVersion) {
        this.minTargetAvroVersion = minTargetAvroVersion;
    }

    public Collection<AvroGeneratedSourceCode> generateCode() {

        //go over inputs, and classify them into 2 groups - importable ("shared")
        //schemas file and non-importable schema files.
        //also keep track of where we found each file for later

        //for every included file, holds the root via which this file was included
        //(which could be the file itself if it was included directly)
        Map<File, File> fileToParent = new HashMap<>();
        Set<File> sharedSchemas = new HashSet<>();
        Set<File> nonSharedSchemas = new HashSet<>();

        findAllIncludedFiles(fileToParent, sharedSchemas, nonSharedSchemas);

        LOG.info("compiling {} schema files ({} of which shared)", nonSharedSchemas.size() + sharedSchemas.size(), sharedSchemas.size());

        //"external" schemas we need during parsing and that come from (for example) the classpath
        Map<String, SchemaDetails> externalSchemas = new HashMap<>();

        //start by parsing the shared schemas
        Map<String, SchemaDetails> sharedParsed = parseSchemas(sharedSchemas, null, externalSchemas);

        //then parse the non-shared ones (while making the shared ones available)
        Map<String, SchemaDetails> nonSharedParsed = parseSchemas(nonSharedSchemas, sharedParsed, externalSchemas);

        //validate all the schemas we parsed
        validateParsedSchemas(sharedParsed, fileToParent);
        validateParsedSchemas(nonSharedParsed, fileToParent);

        //generate java code from all the schemas we just parsed
        List<Schema> allSchemas = new ArrayList<>();
        sharedParsed.values().forEach(schemaDetails -> allSchemas.add(schemaDetails.schema));
        nonSharedParsed.values().forEach(schemaDetails -> allSchemas.add(schemaDetails.schema));
        Collection<AvroGeneratedSourceCode> javaClassFiles = AvroCompatibilityHelper.compile(allSchemas, minTargetAvroVersion);
        Collection<AvroGeneratedSourceCode> nonExternal;

        //filter out java code generated for external schemas
        if (!externalSchemas.isEmpty()) {
            nonExternal = new ArrayList<>(javaClassFiles.size() - sharedSchemas.size());
            for (AvroGeneratedSourceCode javaClass : javaClassFiles) {
                String fqcn = javaClass.getFullyQualifiedClassName();
                if (!externalSchemas.containsKey(fqcn)) {
                    nonExternal.add(javaClass);
                }
            }
        } else {
            nonExternal = javaClassFiles;
        }

        if (outputFolder != null) {
            writeJavaFilesToDisk(nonExternal);
        }

        return nonExternal;
    }

    private void writeJavaFilesToDisk(Collection<AvroGeneratedSourceCode> javaClassFiles) {
        //make sure the output folder exists
        if (!outputFolder.exists() && !outputFolder.mkdirs()) {
            throw new IllegalStateException("unable to create output folder " + outputFolder);
        }

        //write out the files we generated
        for (AvroGeneratedSourceCode javaClass : javaClassFiles) {
            File outputFile = new File(outputFolder, javaClass.getPath());

            if (outputFile.exists()) {
                //TODO - make this behaviour configurable (overwite, ignore, ignore_if_identical, etc)
                throw new IllegalStateException("output file " + outputFile + "already exists");
            } else {
                File parentFolder = outputFile.getParentFile();
                if (!parentFolder.exists() && !parentFolder.mkdirs()) {
                    throw new IllegalStateException("unable to create output folder " + outputFolder);
                }
            }

            try (
                    FileOutputStream fos = new FileOutputStream(outputFile, false);
                    OutputStreamWriter writer = new OutputStreamWriter(fos, StandardCharsets.UTF_8)
            ) {
                writer.write(javaClass.getContents());
                writer.flush();
                fos.flush();
            } catch (Exception e) {
                throw new IllegalStateException("while writing file " + outputFile, e);
            }
        }
    }

    private void validateParsedSchemas(Map<String, SchemaDetails> parsedSchemas, Map<File, File> fileToParent) {
        for (Map.Entry<String, SchemaDetails> entry : parsedSchemas.entrySet()) {
            String fqcn = entry.getKey();
            SchemaDetails schemaDetails = entry.getValue();
            if (!schemaDetails.topLevel) {
                continue;
            }
            Schema schema = schemaDetails.schema;
            File file = schemaDetails.location;
            File root = fileToParent.get(file);

            if (validateSchemaNamespaceVsFilePath) {
                String namespace = schema.getNamespace();
                String relativePath;
                if (root == file) {
                    relativePath = "";
                } else {
                    relativePath = root.toPath().relativize(file.toPath().getParent()).toString().replaceAll(Pattern.quote(File.pathSeparator), ".");
                }
                if (namespace == null) {
                    if (!relativePath.equals("")) {
                        throw new IllegalArgumentException("schema " + schema.getFullName() + " has no namespace yet is defined in "
                                + file + " who's relative path to root is " + relativePath);
                    }
                } else {
                    if (!relativePath.equals(namespace)) {
                        throw new IllegalArgumentException("schema " + schema.getFullName() + " belongs to namespace " + namespace
                                + " yet is defined in " + file + " who's relative path to root is " + relativePath);
                    }
                }
            }

            if (validateSchemaNameVsFileName) {
                String name = schema.getName();
                String fileName = FilenameUtils.removeExtension(file.getName());
                if (!fileName.equals(name)) {
                    throw new IllegalArgumentException("schema " + schema.getFullName() + " has name " + name + " yet is defined in a file called " + file.getName());
                }
            }
        }
    }

    private Map<String, SchemaDetails> parseSchemas(
            Set<File> toParse,
            Map<String, SchemaDetails> importableSchemas,
            Map<String, SchemaDetails> externalSchemas
    ) {
        Collection<File> toTry = toParse;
        Map<String, SchemaDetails> successfullyParsed = new HashMap<>();
        int passNumber = 0;

        //temporary data structures
        List<Schema> allKnownSchemas = new ArrayList<>();
        List<Schema> allKnownTopLevelSchemas = new ArrayList<>();

        while (!toTry.isEmpty()) {
            boolean madeProgress = false;
            passNumber++;
            Map<File, FileParseIssue> failedFiles = new HashMap<>();

            for (File schemaFile : toTry) {
                String fileContents = readFile(schemaFile);
                SchemaParseResult result;
                try {

                    //build up a collection of all known schemas to hand to avro
                    allKnownSchemas.clear();
                    allKnownTopLevelSchemas.clear();
                    buildUpKnownSchemaLists(successfullyParsed, importableSchemas, externalSchemas, allKnownSchemas, allKnownTopLevelSchemas);

                    //call avro to parse our file given all the known schemas we've built up above
                    result = AvroCompatibilityHelper.parse(fileContents, allKnownSchemas);

                    madeProgress = true;
                } catch (SchemaParseException parseException) {
                    failedFiles.put(schemaFile, new FileParseIssue(schemaFile, parseException));
                    continue; //to next file
                } catch (Exception other) {
                    throw new IllegalStateException("while trying to parse file " + schemaFile, other);
                }

                //go over avro results and determine what schemas are new in the file parsed
                Map<String, Schema> actuallyNewSchemas = processParseResults(schemaFile, result, successfullyParsed, importableSchemas, externalSchemas);

                //store new schemas
                actuallyNewSchemas.forEach((fullName, schema) -> {
                    if (successfullyParsed.put(fullName, SchemaDetails.fromFile(schema, schemaFile, schema == result.getMainSchema())) != null) {
                        throw new IllegalStateException();
                    }
                });
            }
            if (!madeProgress) {
                //classify our issues
                failedFiles.forEach((file, issue) -> issue.classification = classifyIssue(issue.exception));
                //terminate for any issues that we cant handle
                throwForFatalErrors(failedFiles, successfullyParsed, importableSchemas);
                //if we got here none of the issues are fatal
                if (allowClasspathLookup) {
                    ClasspathFishingResults fishingResults = goFishingOnTheClasspath(failedFiles);
                    Map<String, SchemaDetails> loot = fishingResults.fqcnsFound;
                    if (!loot.isEmpty()) {
                        //yay, we live to iterate another day!
                        externalSchemas.putAll(fishingResults.fqcnsFound);
                    }
                    else {
                        throw new IllegalArgumentException("unable to find records " + fishingResults.fqcnsNotFound
                                + " used in " + failedFiles.keySet() + ". not even on the classpath");
                    }
                } else {
                    //TODO - improve on this error msg?
                    throw new IllegalArgumentException("cannot make progress. files left: " + failedFiles + ". did not examine classpath");
                }
            }
            toTry = failedFiles.keySet();
        }

        LOG.info("parsed {} schemas out of {} files in {} passes", successfullyParsed.size(), toParse.size(), passNumber);

        return successfullyParsed;
    }

    private ClasspathFishingResults goFishingOnTheClasspath(Map<File, FileParseIssue> failedFiles) {
        Set<String> fqcnsNotFound = new HashSet<>();
        Map<String, String> fqcnsFoundButUnusable = new HashMap<>();
        Map<String, SchemaDetails> fqcnsFound = new HashMap<>();

        for (Map.Entry<File, FileParseIssue> entry : failedFiles.entrySet()) {
            FileParseIssue issue = entry.getValue();
            if (issue.classification.type != IssueType.MISSING_FQCN) {
                continue;
            }
            String fqcn = issue.classification.fqcn;
            Class<?> clazz;
            try {
                clazz = Class.forName(fqcn);
            } catch (ClassNotFoundException ignored) {
                fqcnsNotFound.add(fqcn);
                continue; //no luck
            }

            //is it a avro-generated? does it maybe have a SCHEMA$ field?
            Schema schema = fishForSchemaField(clazz);
            if (schema != null) {
                //yay
                fqcnsFound.put(fqcn, SchemaDetails.fromClasspath(schema, false));
                continue;
            }

            //if we have to go beyond this point we're either dealing with mistaken identity
            //or code generated by old, horrible, avro.

            if (SpecificRecord.class.isAssignableFrom(clazz)) {
                fqcnsFoundButUnusable.put(fqcn, "is a record yet has no SCHEMA$ field??");
            } else if (SpecificFixed.class.isAssignableFrom(clazz)) {
                //if we got here it was generated by old, horrible, avro and not fixed-up to have SCHEMA$.
                if (allowReverseEngineeringClasspathSchemas) {
                    //noinspection unchecked
                    Schema reversed = createFixedSchema((Class<? extends SpecificFixed>) clazz);
                    if (reversed != null) {
                        //yay
                        fqcnsFound.put(fqcn, SchemaDetails.fromClasspath(reversed, true));
                    } else {
                        fqcnsFoundButUnusable.put(fqcn, "has no SCHEMA$ field and unable to reverse the schema?");
                    }
                } else {
                    fqcnsFoundButUnusable.put(fqcn, "has no SCHEMA$ field, and reverse-engineering disabled");
                }
            } else if (Enum.class.isAssignableFrom(clazz)) {
                //if we got here it was generated by old, horrible, avro and not fixed-up to have SCHEMA$.
                //there's no way to tell if this enum was generated by avro or is an innocent by-standar.
                if (allowReverseEngineeringClasspathSchemas) {
                    //noinspection unchecked
                    Schema reversed = createEnumSchema((Class<? extends Enum>) clazz);
                    if (reversed != null) {
                        //yay
                        fqcnsFound.put(fqcn, SchemaDetails.fromClasspath(reversed, true));
                    } else {
                        fqcnsFoundButUnusable.put(fqcn, "has no SCHEMA$ field and unable to reverse the schema?");
                    }
                } else {
                    fqcnsFoundButUnusable.put(fqcn, "has no SCHEMA$ field, and reverse-engineering disabled");
                }
            }
        }

        return new ClasspathFishingResults(fqcnsNotFound, fqcnsFoundButUnusable, fqcnsFound);
    }

    private Schema createFixedSchema(Class<? extends SpecificFixed> clazz) {
        String fqcn = clazz.getCanonicalName();
        FixedSize sizeAnnotation = clazz.getAnnotation(FixedSize.class);
        if (sizeAnnotation == null) {
            throw new IllegalStateException("class " + fqcn + " has no @FixedSize annotation");
        }
        int size = sizeAnnotation.value();

        String template;
        Map<String, String> params = new HashMap<>();
        params.put("size", Integer.toString(size));
        if (fqcn.contains(".")) {
            //has namespace (package name)
            template = FIXED_SCHEMA_TEMPLATE;
            params.put("name", fqcn.substring(fqcn.lastIndexOf('.') + 1));
            params.put("namespace", fqcn.substring(0, fqcn.lastIndexOf('.')));
        } else {
            template = FIXED_SCHEMA_NO_NAMESPACE_TEMPLATE;
            params.put("name", fqcn);
        }
        String avsc = TemplateUtil.populateTemplate(template, params);
        return AvroCompatibilityHelper.parse(avsc);
    }

    private Schema createEnumSchema(Class<? extends Enum> clazz) {
        String fqcn = clazz.getCanonicalName();

        StringJoiner csv = new StringJoiner(", ");
        for (Enum value : clazz.getEnumConstants()) {
            csv.add("\"" + value.name() + "\"");
        }

        String template;
        Map<String, String> params = new HashMap<>();
        params.put("symbols", csv.toString());
        if (fqcn.contains(".")) {
            //has namespace (package name)
            template = ENUM_SCHEMA_TEMPLATE;
            params.put("name", fqcn.substring(fqcn.lastIndexOf('.') + 1));
            params.put("namespace", fqcn.substring(0, fqcn.lastIndexOf('.')));
        } else {
            template = ENUM_SCHEMA_NO_NAMESPACE_TEMPLATE;
            params.put("name", fqcn);
        }
        String avsc = TemplateUtil.populateTemplate(template, params);
        return AvroCompatibilityHelper.parse(avsc);
    }

    private Schema fishForSchemaField(Class<?> clazz) {
        Field schemaField;
        try {
            schemaField = clazz.getField("SCHEMA$");
            if (!Modifier.isStatic(schemaField.getModifiers())) {
                throw new IllegalStateException("class " + clazz.getCanonicalName() + " has SCHEMA$ field that isnt static??");
            }
            return (Schema) schemaField.get(null);
        } catch (NoSuchFieldException nope) {
            //at least we tried
            return null;
        } catch (Exception oops) {
            throw new IllegalStateException("while trying to access " + clazz.getCanonicalName() + ".SCHEMA$", oops);
        }
    }

    private void throwForFatalErrors(
            Map<File, FileParseIssue> failedFiles,
            Map<String, SchemaDetails> successfullyParsed,
            Map<String, SchemaDetails> importableSchemas
    ) {

        List<RuntimeException> toThrow = new ArrayList<>();

        for (Map.Entry<File, FileParseIssue> entry : failedFiles.entrySet()) {
            File file = entry.getKey();
            FileParseIssue issue = entry.getValue();
            String fqcn = issue.classification.fqcn;

            switch (issue.classification.type) {
                case REDEFINITION:
                    //be nice to users, figure out where the other definition is
                    SchemaDetails dup = successfullyParsed.get(fqcn);
                    if (dup == null && importableSchemas != null) {
                        dup = importableSchemas.get(fqcn);
                    }
                    if (dup == null) {
                        //something is wrong with the universe
                        toThrow.add(new IllegalStateException("avro claims dup in " + file + " but we cant see where", issue.exception));
                        break;
                    }
                    toThrow.add(new IllegalStateException("schema " + fqcn + " is defined in both " + file + " and " + dup.location, issue.exception));
                    break;
                case MISSING_FQCN:
                    if (!allowClasspathLookup) {
                        toThrow.add(new IllegalStateException("record " + fqcn + " referenced in " + file + " not found and classpath lookup is off", issue.exception));
                    }
                    break;
                case OTHER:
                    toThrow.add(issue.exception); //we cant handle this
                    break;
                default:
                    //this is a bug
                    throw new IllegalStateException("unhandled: " + issue.classification.type, issue.exception);
            }
        }

        if (toThrow.isEmpty()) {
            return;
        }
        RuntimeException first = toThrow.get(0);
        if (toThrow.size() > 1) {
            for (int i = 1; i < toThrow.size(); i++) {
                first.addSuppressed(toThrow.get(i));
            }
        }
        throw first;
    }

    private ClassifiedIssue classifyIssue(SchemaParseException issue) {
        String msg = issue.getMessage();
        if (msg == null) {
            return new ClassifiedIssue(IssueType.OTHER, null);
        }
        Matcher matcher = SCHEMA_REDEFINITION.matcher(msg);
        if (matcher.matches()) {
            String fqcn = matcher.group(1);
            return new ClassifiedIssue(IssueType.REDEFINITION, fqcn);
        }
        matcher = UNDEFINED_NAME.matcher(msg);
        if (matcher.find()) {
            String fqcn = matcher.group(1);
            return new ClassifiedIssue(IssueType.MISSING_FQCN, fqcn);
        }
        matcher = UNDEFINED_NAME_IN_UNION.matcher(msg);
        if (matcher.find()) {
            String fqcn = matcher.group(1);
            return new ClassifiedIssue(IssueType.MISSING_FQCN, fqcn);
        }
        return new ClassifiedIssue(IssueType.OTHER, null);
    }

    private Map<String, Schema> processParseResults(
            File schemaFile,
            SchemaParseResult result,
            Map<String, SchemaDetails> successfullyParsed,
            Map<String, SchemaDetails> moreSchemas,
            Map<String, SchemaDetails> externalSchemas
    ) {

//        Schema mainSchema = result.getMainSchema();
        Map<String, Schema> allSchemas = result.getAllSchemas(); //will contain all schemas defined anywhere

        Map<String, Schema> actuallyNewSchemas = new HashMap<>();

//        String mainSchemaFullName = mainSchema.getFullName();
//        SchemaDetails alreadyDefinedMain = successfullyParsed.get(mainSchemaFullName);
//        if (alreadyDefinedMain != null) {
//            throw new IllegalArgumentException("schema " + mainSchemaFullName + " is defined in " + schemaFile + " and also in " + alreadyDefinedMain.location);
//        }
//        alreadyDefinedMain = moreSchemas.get(mainSchemaFullName);
//        if (alreadyDefinedMain != null) {
//            throw new IllegalArgumentException("schema " + mainSchemaFullName + " is defined in " + schemaFile + " and also in " + alreadyDefinedMain.location);
//        }

        for (Map.Entry<String, Schema> entry : allSchemas.entrySet()) {
            String fullName = entry.getKey();
            Schema schema = entry.getValue();

            SchemaDetails alreadyDefined = successfullyParsed.get(fullName);
            if (alreadyDefined != null) {
                if (alreadyDefined.schema == schema) { //yes, pointer comparison, not equals()
                    //not a redefinition, just a known schema
                    continue;
                }
                throw new IllegalArgumentException("schema " + fullName + " is defined in " + schemaFile + " and also in " + alreadyDefined.location);
            }
            if (moreSchemas != null) {
                alreadyDefined = moreSchemas.get(fullName);
                if (alreadyDefined != null) {
                    if (alreadyDefined.schema == schema) { //yes, pointer comparison, not equals()
                        //not a redefinition, just a known schema
                        continue;
                    }
                    throw new IllegalArgumentException("schema " + fullName + " is defined in " + schemaFile + " and also in " + alreadyDefined.location);
                }
            }
            if (externalSchemas != null) {
                alreadyDefined = externalSchemas.get(fullName);
                if (alreadyDefined != null) {
                    if (alreadyDefined.schema == schema) { //yes, pointer comparison, not equals()
                        //not a redefinition, just a known schema
                        continue;
                    }
                    throw new IllegalArgumentException("schema " + fullName + " is defined in " + schemaFile + " and also on the classpath");
                }
            }

            actuallyNewSchemas.put(schema.getFullName(), schema);
        }

        return actuallyNewSchemas;
    }

    private void buildUpKnownSchemaLists(
            Map<String, SchemaDetails> successfullyParsed,
            Map<String, SchemaDetails> moreSchemas,
            Map<String, SchemaDetails> externalSchemas,
            List<Schema> allKnownSchemas,
            List<Schema> allKnownTopLevelSchemas
    ) {
        successfullyParsed.forEach((fullName, schemaDetails) -> {
            if (schemaDetails.topLevel) {
                allKnownTopLevelSchemas.add(schemaDetails.schema);
            }
            allKnownSchemas.add(schemaDetails.schema);
        });
        if (moreSchemas != null) {
            moreSchemas.forEach((fullName, schemaDetails) -> {
                if (schemaDetails.topLevel) {
                    allKnownTopLevelSchemas.add(schemaDetails.schema);
                }
                allKnownSchemas.add(schemaDetails.schema);
            });
        }
        if (externalSchemas != null) {
            externalSchemas.forEach((fullName, schemaDetails) -> allKnownSchemas.add(schemaDetails.schema));
        }
    }

    private void findAllIncludedFiles(Map<File, File> fileToParent, Set<File> sharedSchemas, Set<File> nonSharedSchemas) {
        String[] includesArray = includes.toArray(new String[0]);

        for (File input : inputs) {
            if (!input.exists()) {
                LOG.warn("path {} does not exist", input);
            }

            if (input.isFile()) {
                //individual files are always considered included, so no test against includes
                includeFile(input, input, fileToParent, sharedSchemas, nonSharedSchemas);
            } else if (input.isDirectory()) {
                DirectoryScanner scanner = new DirectoryScanner();
                scanner.setBasedir(input);
                scanner.setIncludes(includesArray);
                scanner.scan();
                //results are paths relative (==under) input
                String[] includedFiles = scanner.getIncludedFiles();
                for (String included : includedFiles) {
                    File file = new File(input, included);
                    includeFile(file, input, fileToParent, sharedSchemas, nonSharedSchemas);
                }
            } else {
                LOG.warn("path {} is neither a file nor a directory", input);
            }
        }
    }

    private void includeFile(File file, File root, Map<File, File> fileToParent, Set<File> sharedSchemas, Set<File> nonSharedSchemas) {
        File otherParent = fileToParent.putIfAbsent(file, root);
        if (otherParent != null) {
            String includedHow = otherParent.equals(file) ? "individually" : "via " + otherParent;
            throw new IllegalArgumentException("file " + file + " was already included " + includedHow);
        }
        String relativePath;
        if (root == file) {
            relativePath = file.getName();
        } else {
            relativePath = root.toPath().relativize(file.toPath()).toString();
        }
        boolean shared = isSharedSchema(relativePath);
        Set<File> collection = shared ? sharedSchemas : nonSharedSchemas;
        if (!collection.add(file)) {
            throw new IllegalStateException(file + " included twice?!");
        }
    }

    private String readFile(File file) {
        try (FileInputStream is = new FileInputStream(file)) {
            return IOUtils.toString(is, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("while trying to read " + file, e);
        }
    }

    private boolean isSharedSchema(String pathRelativeToRoot) {
        for (String importPattern : importablePatterns) {
            if (SelectorUtils.matchPath(importPattern, pathRelativeToRoot)) {
                return true;
            }
        }
        return false;
    }

    private static class ClasspathFishingResults {
        private final Set<String> fqcnsNotFound;
        private final Map<String, String> fqcnsFoundButUnusable;
        private final Map<String, SchemaDetails> fqcnsFound;

        public ClasspathFishingResults(
                Set<String> fqcnsNotFound,
                Map<String, String> fqcnsFoundButUnusable,
                Map<String, SchemaDetails> fqcnsFound
        ) {
            this.fqcnsNotFound = fqcnsNotFound;
            this.fqcnsFoundButUnusable = fqcnsFoundButUnusable;
            this.fqcnsFound = fqcnsFound;
        }

        public Set<String> getFqcnsNotFound() {
            return fqcnsNotFound;
        }

        public Map<String, String> getFqcnsFoundButUnusable() {
            return fqcnsFoundButUnusable;
        }

        public Map<String, SchemaDetails> getFqcnsFound() {
            return fqcnsFound;
        }
    }

    private enum SchemaSource {
        FILE, CLASSPATH
    }

    private static class SchemaDetails {
        private Schema schema;
        private SchemaSource source;

        //fields valid for schemas sourced from files
        private File location;
        private boolean topLevel;

        //fields valid for schemas sources from the classpath
        private boolean reversed;

        static SchemaDetails fromFile(Schema schema, File location, boolean topLevel) {
            return new SchemaDetails(schema, SchemaSource.FILE, location, topLevel, false);
        }

        static SchemaDetails fromClasspath(Schema schema, boolean reversed) {
            return new SchemaDetails(schema, SchemaSource.CLASSPATH, null, false, reversed);
        }

        private SchemaDetails(Schema schema, SchemaSource source, File location, boolean topLevel, boolean reversed) {
            this.schema = schema;
            this.source = source;
            this.location = location;
            this.topLevel = topLevel;
            this.reversed = reversed;
        }

        public Schema getSchema() {
            return schema;
        }

        public SchemaSource getSource() {
            return source;
        }

        public File getLocation() {
            return location;
        }

        public boolean isTopLevel() {
            return topLevel;
        }
    }

    private static class FileParseIssue {
        private final File file;
        private final SchemaParseException exception;
        private ClassifiedIssue classification;

        public FileParseIssue(File file, SchemaParseException exception) {
            this.file = file;
            this.exception = exception;
        }
    }

    private enum IssueType {
        REDEFINITION, MISSING_FQCN, OTHER
    }

    private static class ClassifiedIssue {
        private final IssueType type;
        private final String fqcn;

        public ClassifiedIssue(IssueType type, String fqcn) {
            this.type = type;
            this.fqcn = fqcn;
        }
    }
}
