/*
 * Copyright 2023 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.normalization;

import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelper;
import com.linkedin.avroutil1.compatibility.AvroCompatibilityHelperCommon;
import com.linkedin.avroutil1.compatibility.AvroVersion;
import com.linkedin.avroutil1.compatibility.AvscGenerationConfig;
import com.linkedin.avroutil1.compatibility.AvscWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import net.openhft.hashing.LongTupleHashFunction;
import org.apache.avro.Schema;


public class AvroUtilSchemaNormalization {

  public enum FingerprintingAlgo {
    CRC_64 ("CRC-64-AVRO"),
    MD5_128 ("MD5"),
    XX_128("XX_HASH_128"),
    SHA_256 ("SHA-256");

    private final String name;
    FingerprintingAlgo(String fpName) {
      name = fpName;
    }

    public String toString() { return name; }
  }

  /**
   * Returns "Parsing Canonical Form" of a schema as defined by Avro spec.
   * @param schema an avro schema
   * @param config generation config
   * @param schemaPlugins List of AvscWriterPlugins to be applied on the schema during avsc generation
   * @return parsing canonical form of the schema
   */
  public static String getCanonicalForm(Schema schema, AvscGenerationConfig config,
      List<AvscWriterPlugin> schemaPlugins) {
    if (AvroCompatibilityHelperCommon.getRuntimeAvroVersion().laterThan(AvroVersion.AVRO_1_6)) {
      AvscWriter writer = AvroCompatibilityHelper.getAvscWriter(getNonLegacyConfig(config), schemaPlugins);
      return writer.toAvsc(schema);
    }
    else {
      throw new UnsupportedOperationException("Canonicalize need avro >= 1.7");
    }
  }

  /**
   * Returns {@link #fingerprint} applied to the parsing canonical form of the
   * supplied schema.
   * @param fpName fingerprint algorithm name
   * @param s schema who's parsing canonical form will be fingerprinted
   * @return fingerprint of given schema's parsing canonical form
   * @throws NoSuchAlgorithmException if algorithm if not found, shouldn't happen
   */
  public static byte[] parsingFingerprint(FingerprintingAlgo fpName, Schema s, AvscGenerationConfig config,
      List<AvscWriterPlugin> plugins) throws NoSuchAlgorithmException {
    return fingerprint(fpName, getCanonicalForm(s, getNonLegacyConfig(config), plugins).getBytes(StandardCharsets.UTF_8));
  }

  /**
   * Returns {@link #fingerprint} applied to the parsing canonical form of the
   * supplied schema.
   * @param fpName fingerprint algorithm name
   * @param s schema who's parsing canonical form will be fingerprinted
   * @return fingerprint of given schema's parsing canonical form
   * @throws NoSuchAlgorithmException if algorithm if not found, shouldn't happen
   */
  public static byte[] parsingFingerprint(FingerprintingAlgo fpName, Schema s) throws NoSuchAlgorithmException {
    return fingerprint(fpName,
        getCanonicalForm(s, AvscGenerationConfig.CANONICAL_BROAD_ONELINE, new ArrayList<AvscWriterPlugin>(0)).getBytes(
            StandardCharsets.UTF_8));
  }

  /**
   * Returns {@link #fingerprint64} applied to the parsing canonical form of the
   * supplied schema.
   * @param s schema who's parsing canonical form will be fingerprinted
   * @return 64bit fingerprint of given schemas parsing canonical form
   */
  public static long parsingFingerprint64(Schema s) {
    return fingerprint64(
        getCanonicalForm(s, AvscGenerationConfig.CANONICAL_BROAD_ONELINE, new ArrayList<AvscWriterPlugin>(0)).getBytes(
            StandardCharsets.UTF_8));
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
   * @throws NoSuchAlgorithmException if algorithm by provided name not found, shouldn't happen
   */
  public static byte[] fingerprint(FingerprintingAlgo fpName, byte[] data) throws NoSuchAlgorithmException {
    if (FingerprintingAlgo.CRC_64.equals(fpName)) {
      long fp = fingerprint64(data);
      byte[] result = new byte[8];
      for (int i = 0; i < 8; i++) {
        result[i] = (byte) fp;
        fp >>= 8;
      }
      return result;
    } else if (FingerprintingAlgo.XX_128.equals(fpName)) {
      return toByteArray(LongTupleHashFunction.xx128().hashBytes(data));
    }
    MessageDigest md = MessageDigest.getInstance(fpName.toString());
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

  private static byte[] toByteArray(long[] longArray) {
    ByteBuffer bb = ByteBuffer.allocate(longArray.length * Long.BYTES);
    bb.asLongBuffer().put(longArray);
    return bb.array();
  }

  /**
   * Converts config to non legacy. Keeps all options except isLegacy which turns to false
   * @param config config to be converted to non legacy.
   * @return non legacy config
   */
  private static AvscGenerationConfig getNonLegacyConfig(AvscGenerationConfig config) {
    return new AvscGenerationConfig(config.isPreferUseOfRuntimeAvro(), config.isForceUseOfRuntimeAvro(),
        config.isPrettyPrint(), config.getRetainPreAvro702Logic(), config.isAddAvro702Aliases(),
        config.retainDefaults, config.retainDocs, config.retainFieldAliases, config.retainNonClaimedProps,
        config.retainSchemaAliases, config.writeNamespaceExplicitly, config.writeRelativeNamespace,
        false);
  }
}
