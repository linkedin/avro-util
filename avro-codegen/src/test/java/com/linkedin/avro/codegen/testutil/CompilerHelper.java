/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.codegen.testutil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.tools.Diagnostic;
import javax.tools.DiagnosticListener;
import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;
import org.apache.commons.io.IOUtils;
import org.testng.Assert;

/**
 * utility class for dealing with the java compiler in unit tests
 */
public class CompilerHelper {

    public static void assertCompiles(Path sourceRoot) throws Exception {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        ClassFileManager manager = new ClassFileManager(compiler.getStandardFileManager(null, null, null));
        CompilationListener listener = new CompilationListener();
        List<JavaSourceFile> fileObjects = listSourceFiles(sourceRoot);
        JavaCompiler.CompilationTask compilationTask = compiler.getTask(null, manager, listener, null, null, fileObjects);
        Boolean success = compilationTask.call();

        if (!Boolean.TRUE.equals(success) || !listener.errors.isEmpty()) {
            Assert.fail("failed to compile code under " + sourceRoot + ": " + listener.errors);
        }
    }

    private static List<JavaSourceFile> listSourceFiles(Path root) throws IOException {
        List<JavaSourceFile> fileObjects = new ArrayList<>();
        //noinspection Convert2Diamond because java 8 isnt smart enough to figure this out
        Files.walkFileTree(root, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (attrs.isRegularFile()) {
                    fileObjects.add(new JavaSourceFile(file));
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return fileObjects;
    }

    private static String summarize(Diagnostic<? extends JavaFileObject> diagnostic) {
        StringBuilder sb = new StringBuilder();
        sb.append(diagnostic.getKind()).append(": ");
        JavaFileObject sourceObject = diagnostic.getSource();
        if (sourceObject != null) {
            sb.append("file ").append(sourceObject.toString()).append(" ");
        }
        String message = diagnostic.getMessage(Locale.ROOT);
        if (message != null && !message.isEmpty()) {
            sb.append(message).append(" ");
        }
        long line = diagnostic.getLineNumber();
        long column = diagnostic.getColumnNumber();
        if (line != -1 || column != -1) {
            sb.append("at line ").append(line).append(" column ").append(column);
        }
        return sb.toString().trim();
    }

    private static class ClassFileManager extends ForwardingJavaFileManager<StandardJavaFileManager> {
        private final Map<String, JavaClassObject> classObjects = new ConcurrentHashMap<>();

        ClassFileManager(StandardJavaFileManager m) {
            super(m);
        }

        @Override
        public JavaFileObject getJavaFileForOutput(
                JavaFileManager.Location location,
                String className,
                JavaFileObject.Kind kind,
                FileObject sibling
        ) throws IOException {
            if (kind != JavaFileObject.Kind.CLASS) {
                throw new UnsupportedOperationException("unhandled kind " + kind);
            }
            return classObjects.computeIfAbsent(className, s -> new JavaClassObject((JavaSourceFile) sibling, className));
        }

    }

    private static class JavaSourceFile extends SimpleJavaFileObject {
        private final Path file;

        JavaSourceFile(Path file) {
            super(file.toUri(), Kind.SOURCE);
            this.file = file;
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
            return IOUtils.toString(file.toUri(), StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return file.getFileName().toString();
        }
    }

    private static class JavaClassObject extends SimpleJavaFileObject {
        private final JavaSourceFile sourceFile;
        private final String fqcn;
        private volatile ByteArrayOutputStream outputStream;

        public JavaClassObject(JavaSourceFile sourceFile, String fqcn) {
            super(URI.create("mem:///" + fqcn.replace('.', '/')), Kind.CLASS);
            this.sourceFile = sourceFile;
            this.fqcn = fqcn;
        }

        @Override
        public synchronized OutputStream openOutputStream() {
            if (outputStream == null) {
                outputStream = new ByteArrayOutputStream();
            }
            return outputStream;
        }
    }

    private static class CompilationListener implements DiagnosticListener<JavaFileObject> {
        private final List<String> warnings = new ArrayList<>();
        private final List<String> errors = new ArrayList<>();

        @Override
        public void report(Diagnostic<? extends JavaFileObject> diagnostic) {
            Diagnostic.Kind kind = diagnostic.getKind();
            String desc = summarize(diagnostic);
            switch (kind) {
                case NOTE:
                case WARNING:
                case MANDATORY_WARNING:
                    warnings.add(desc);
                    break;
                case ERROR:
                case OTHER:
                    errors.add(desc);
                    break;
                default:
                    throw new IllegalStateException("unhandled " + kind);
            }
        }
    }
}
