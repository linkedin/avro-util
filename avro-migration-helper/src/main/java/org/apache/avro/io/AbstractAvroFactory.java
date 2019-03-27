/*
 * Copyright 2018 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License"). 
 * See License in the project root for license information.
 */

package org.apache.avro.io;

import com.linkedin.avro.compatibility.AvroGeneratedSourceCode;
import com.linkedin.avro.compatibility.AvroVersion;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.Schema;


//package private ON PURPOSE
abstract class AbstractAvroFactory implements AvroFactory {
  //common fields
  protected final Constructor _enumSymbolCtr;
  protected final Constructor _fixedCtr;

  //common compiler fields
  protected Constructor _specificCompilerCtr;
  protected Method _compilerEnqueueMethod;
  protected Method _compilerCompileMethod;
  protected Field _outputFilePathField;
  protected Field _outputFileContentsField;

  public AbstractAvroFactory(Constructor enumSymbolCtr, Constructor fixedCtr) throws Exception {
    _enumSymbolCtr = enumSymbolCtr;
    _fixedCtr = fixedCtr;
  }

  @Override
  public Collection<AvroGeneratedSourceCode> compile(Collection<Schema> toCompile, AvroVersion compatibilityTarget) {
    if (toCompile == null || toCompile.isEmpty()) {
      return Collections.emptyList();
    }
    Iterator<Schema> schemaIter = toCompile.iterator();
    Schema first = schemaIter.next();
    try {
      Object compilerInstance = getCompilerInstance(first, compatibilityTarget);

      while (schemaIter.hasNext()) {
        _compilerEnqueueMethod.invoke(compilerInstance, schemaIter.next());
      }
      Collection<?> outputFiles = (Collection<?>) _compilerCompileMethod.invoke(compilerInstance);
      List<AvroGeneratedSourceCode> translated = outputFiles.stream()
          .map(o -> new AvroGeneratedSourceCode(getPath(o), getContents(o)))
          .collect(Collectors.toList());

      return transform(translated, compatibilityTarget);
    } catch (UnsupportedOperationException e) {
      throw e; //as-is
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  protected Object getCompilerInstance(Schema firstSchema, AvroVersion compatibilityTarget) throws Exception {
    return _specificCompilerCtr.newInstance(firstSchema);
  }

  protected List<AvroGeneratedSourceCode> transform(List<AvroGeneratedSourceCode> avroCodegenOutput, AvroVersion compatibilityLevel) {
    return avroCodegenOutput; //nop
  }

  protected String getPath(Object shouldBeOutputFile) {
    try {
      return (String) _outputFilePathField.get(shouldBeOutputFile);
    } catch (Exception e) {
      throw new IllegalStateException("cant extract path from avro OutputFile", e);
    }
  }

  protected String getContents(Object shouldBeOutputFile) {
    try {
      return (String) _outputFileContentsField.get(shouldBeOutputFile);
    } catch (Exception e) {
      throw new IllegalStateException("cant extract contents from avro OutputFile", e);
    }
  }
}
