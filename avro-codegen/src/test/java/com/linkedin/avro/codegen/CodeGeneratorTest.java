/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.codegen;

import com.linkedin.avro.codegen.testutil.CompilerHelper;
import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import net.openhft.compiler.CompilerUtils;
import org.apache.avro.specific.SpecificFixed;
import org.apache.avro.specific.SpecificRecord;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class CodeGeneratorTest {
    private File sourceRoot;
    private Path outputRoot;
    private CodeGenerator generator;

    @BeforeMethod
    public void beforeTest(Method testMethod) throws Exception {
        String methodName = testMethod.getName();
        String rootFolderForMethod = methodName.substring(4, 5).toLowerCase(Locale.ROOT) + methodName.substring(5);
        File cwd = new File(".");
        File root = new File(cwd, "src/test/projects/" + rootFolderForMethod);
        if (!root.exists() || !root.isDirectory()) {
            Assert.fail("unable to find " + root + " for method " + methodName + ". cwd is " + cwd.getCanonicalPath());
        }
        this.sourceRoot = root.exists() ? root : null;
        this.outputRoot = Files.createTempDirectory(getClass().getSimpleName() + "." + methodName);
        this.generator = new CodeGenerator();
        this.generator.setInputs(sourceRoot);
        this.generator.setOutputFolder(outputRoot.toFile());
    }

    @Test
    public void testSimpleProject() throws Exception {
        generator.setImportablePatterns(new HashSet<>(Collections.singletonList("shared/**")));
        generator.setValidateSchemaNamespaceVsFilePath(false);
        generator.setValidateSchemaNameVsFileName(true);

        Collection<AvroGeneratedSourceCode> sourceFiles = generator.generateCode();
        Assert.assertEquals(sourceFiles.size(), 4);

        CompilerHelper.assertCompiles(outputRoot);
    }

    @Test
    public void testDuplicateDefinition() throws Exception {
        generator.setValidateSchemaNameVsFileName(false);
        generator.setValidateSchemaNamespaceVsFilePath(false);

        try {
            generator.generateCode();
            Assert.fail("call was supposed to fail");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("com.acme.Record"));
            Assert.assertTrue(expected.getMessage().contains("File1"));
            Assert.assertTrue(expected.getMessage().contains("File2"));
        }
    }

    @Test
    public void testDuplicateInnerDefinition() throws Exception {
        generator.setValidateSchemaNameVsFileName(false);
        generator.setValidateSchemaNamespaceVsFilePath(false);

        try {
            generator.generateCode();
            Assert.fail("call was supposed to fail");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("com.acme.Inner"));
            Assert.assertTrue(expected.getMessage().contains("Record1"));
            Assert.assertTrue(expected.getMessage().contains("Record2"));
        }
    }

    @Test
    public void testProjectWithClasspathDependencies() throws Exception {
        //first we code-gen, compile AND LOAD the 1st module
        generator.setInputs(new File(sourceRoot, "firstModule"));
        generator.setOutputFolder(null); //no need to write java source files to disk
        Collection<AvroGeneratedSourceCode> firstModuleSources = generator.generateCode();
        Assert.assertEquals(firstModuleSources.size(), 3);

        Class enumClass = null;
        Class fixedClass = null;
        Class recordClass = null;

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (AvroGeneratedSourceCode code : firstModuleSources) {
            String fqcn = code.getFullyQualifiedClassName();
            switch (fqcn) {
                case "com.acme.ClasspathEnum":
                    enumClass = CompilerUtils.CACHED_COMPILER.loadFromJava(classLoader, fqcn, code.getContents());
                    break;
                case "com.acme.ClasspathFixed":
                    fixedClass = CompilerUtils.CACHED_COMPILER.loadFromJava(classLoader, fqcn, code.getContents());
                    break;
                case "com.acme.ClasspathRecord":
                    recordClass = CompilerUtils.CACHED_COMPILER.loadFromJava(classLoader, fqcn, code.getContents());
                    break;
                default:
                    throw new IllegalStateException("unhandled " + fqcn);
            }
        }
        Assert.assertNotNull(enumClass);
        Assert.assertNotNull(fixedClass);
        Assert.assertNotNull(recordClass);

        //make sure the class loaded represents a generated avro record
        Assert.assertTrue(Enum.class.isAssignableFrom(enumClass));
        Assert.assertTrue(SpecificFixed.class.isAssignableFrom(fixedClass));
        Assert.assertTrue(SpecificRecord.class.isAssignableFrom(recordClass));
        //and that its now on our classpath
        Assert.assertEquals(Class.forName("com.acme.ClasspathEnum"), enumClass);
        Assert.assertEquals(Class.forName("com.acme.ClasspathFixed"), fixedClass);
        Assert.assertEquals(Class.forName("com.acme.ClasspathRecord"), recordClass);

        //now go code-gen and compile the 2nd module
        generator.setInputs(new File(sourceRoot, "secondModule"));
        generator.setAllowClasspathLookup(true);

        generator.setOutputFolder(outputRoot.toFile()); //these we want on disk
        Collection<AvroGeneratedSourceCode> secondModuleSources = generator.generateCode();
        Assert.assertEquals(secondModuleSources.size(), 6);
        //TODO - figure out how to make the below code pick up classes dynamically defined above
        //CompilerHelper.assertCompiles(outputRoot);
    }
}
