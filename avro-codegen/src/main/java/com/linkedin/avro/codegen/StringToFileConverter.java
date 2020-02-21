/*
 * Copyright 2020 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avro.codegen;

import com.beust.jcommander.IStringConverter;
import java.io.File;


public class StringToFileConverter implements IStringConverter<File> {
  @Override
  public File convert(String value) {
    return new File(value);
  }
}