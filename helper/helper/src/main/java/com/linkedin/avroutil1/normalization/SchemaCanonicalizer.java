/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.normalization;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.AvscWriter;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import org.apache.avro.Schema;


public class SchemaCanonicalizer {

  /**
   * Returns "Parsing Canonical Form" of a schema as defined by Avro spec.
   * @param schema an avro schema
   * @param config generation config
   * @param schemaPlugins List of AvscWriterPlugins to be applied on the schema during avsc generation
   * @return parsing canonical form of the schema
   */
  public static String getCanonicalForm(Schema schema, AvscGenerationConfig config, List<AvscWriterPlugin> schemaPlugins) {
    if (AvroCompatibilityHelper.getRuntimeAvroCompilerVersion().laterThan(AvroVersion.AVRO_1_4)) {
      AvscWriter writer = AvroCompatibilityHelper.getAvscWriter(config, schemaPlugins);
      return writer.toAvsc(schema);
    }
    else {
      throw new UnsupportedOperationException("Canonicalize need avro > 1.4");
    }
  }

  /**
   * Returns a fingerprint of a string of bytes. This string is presumed to
   * contain a canonical form of a schema. The algorithm used to compute the
   * fingerprint is selected by the argument <i>fpName</i>. If <i>fpName</i>
   * equals the string <code>"CRC-64-AVRO"</code>, then the result of
   * {@link #fingerprint64} is returned in little-endian format. Otherwise,
   * <i>fpName</i> is used as an algorithm name for
   * {@link MessageDigest#getInstance(String)}, which will throw
   * <code>NoSuchAlgorithmException</code> if it doesn't recognize the name.
   * <p>
   * Recommended Avro practice dictates that <code>"CRC-64-AVRO"</code> is used
   * for 64-bit fingerprints, <code>"MD5"</code> is used for 128-bit fingerprints,
   * and <code>"SHA-256"</code> is used for 256-bit fingerprints.
   * @param fpName fingerprint algorithm name. typically "CRC-64-AVRO", "MD5" or "SHA-256"
   * @param data bytes to fingerprint
   * @return the fingerprint (size depends on algorithm)
   * @throws NoSuchAlgorithmException if algorithm by provided name not found
   */
  public static byte[] fingerprint(String fpName, byte[] data) throws NoSuchAlgorithmException {
    if (fpName.equals("CRC-64-AVRO")) {
      long fp = fingerprint64(data);
      byte[] result = new byte[8];
      for (int i = 0; i < 8; i++) {
        result[i] = (byte) fp;
        fp >>= 8;
      }
      return result;
    }
    MessageDigest md = MessageDigest.getInstance(fpName);
    return md.digest(data);
  }

  /**
   * Returns the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a
   * byte string.
   * @param data bytes to fingerprint
   * @return rabin fingerprint for given bytes
   */
  public static long fingerprint64(byte[] data) {
    long result = EMPTY64;
    for (byte b : data)
      result = (result >>> 8) ^ FP64.FP_TABLE[(int) (result ^ b) & 0xff];
    return result;
  }

  final static long EMPTY64 = 0xc15d213aa4d7a795L;
  /* An inner class ensures that FP_TABLE initialized only when needed. */
  private static class FP64 {
    private static final long[] FP_TABLE = new long[256];
    static {
      for (int i = 0; i < 256; i++) {
        long fp = i;
        for (int j = 0; j < 8; j++) {
          long mask = -(fp & 1L);
          fp = (fp >>> 1) ^ (EMPTY64 & mask);
        }
        FP_TABLE[i] = fp;
      }
    }
  }
}
