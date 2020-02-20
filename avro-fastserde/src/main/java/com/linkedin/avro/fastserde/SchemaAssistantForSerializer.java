package com.linkedin.avro.fastserde;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;


public class SchemaAssistantForSerializer extends SchemaAssistant {
  public SchemaAssistantForSerializer(JCodeModel codeModel, boolean useGenericTypes) {
    super(codeModel, useGenericTypes);
  }

  /**
   * Special handling for "String" type since the underlying data could be "String" or "Utf8".
   *
   * This is different from the de-serializer since Avro will always decode it into "Utf8".
   * @return the String type to use for serializing
   */
  @Override
  protected JClass defaultStringType() {
    return getCodeModel().ref(CharSequence.class);
  }
}
