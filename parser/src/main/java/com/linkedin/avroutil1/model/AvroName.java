/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.model;

import java.util.Objects;


public class AvroName {
  private final static String NO_NAMESPACE = "";

  private final String simpleName;
  private final String namespace;
  private final String fullname;

  public AvroName(String simpleName, String namespace) {
    if (simpleName == null || simpleName.isEmpty()) {
      throw new IllegalArgumentException("simple name required");
    }
    if (simpleName.contains(".")) {
      throw new IllegalArgumentException("simple name must be simple: " + simpleName);
    }
    this.simpleName = simpleName;
    if (namespace != null && !namespace.isEmpty()) {
      this.namespace = namespace;
      this.fullname = namespace + "." + simpleName;
    } else {
      this.namespace = NO_NAMESPACE;
      this.fullname = this.simpleName;
    }
  }

  public String getSimpleName() {
    return simpleName;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getFullname() {
    return fullname;
  }

  public boolean hasNamespace() {
    return !NO_NAMESPACE.equals(namespace);
  }

  public String qualified(String contextNamespace) {
    if (!namespace.equals(contextNamespace)) { //also works if argument is null
      return fullname;
    }
    return simpleName;
  }

  @Override
  public String toString() {
    return fullname;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AvroName avroName = (AvroName) o;
    return Objects.equals(fullname, avroName.fullname);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fullname);
  }
}
