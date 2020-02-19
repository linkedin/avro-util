package org.apache.avro.io.parsing;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.avro.io.Encoder;
import org.codehaus.jackson.JsonNode;


/**
 * this class exists to allow access to package-private methods on {@link ResolvingGrammarGenerator}
 */
public class Avro14ResolvingGrammarGeneratorAccessUtil {

  private Avro14ResolvingGrammarGeneratorAccessUtil() {
    //util class
  }

  public static void encode(Encoder e, Schema s, JsonNode n) throws IOException {
    ResolvingGrammarGenerator.encode(e, s, n);
  }
}
