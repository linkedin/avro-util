/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.compatibility.avro111;

import com.linkedin.avroutil1.testcommon.TestUtil;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * this class generates test payloads based on the avro schemas defined in this module
 * under avro 1.6 in various wire formats. these payloads are then available for use
 * by the modules containing the actual unit tests
 */
public class Generate111TestResources {

    public static void main(String[] args) {
        if (args == null || args.length != 1) {
            System.err.println("exactly single argument required - output path. instead got " + Arrays.toString(args));
            System.exit(1);
        }
        Path outputRoot = Paths.get(args[0].trim()).toAbsolutePath();
        Path by111Root = outputRoot.resolve("by111");

        by111.RecordWithUnion outer = new by111.RecordWithUnion();
        outer.setF(new by111.InnerUnionRecord());
        outer.getF().setF(111);
        try {
            SpecificDatumWriter<by111.RecordWithUnion> writer = new SpecificDatumWriter<>(outer.getSchema());

            Path binaryRecordWithUnion = TestUtil.getNewFile(by111Root, "RecordWithUnion.binary");
            BinaryEncoder binaryEnc = EncoderFactory.get().binaryEncoder(Files.newOutputStream(binaryRecordWithUnion), null);

            Path jsonRecordWithUnion = TestUtil.getNewFile(by111Root, "RecordWithUnion.json");
            JsonEncoder jsonEnc = EncoderFactory.get().jsonEncoder(outer.getSchema(), Files.newOutputStream(jsonRecordWithUnion));

            writer.write(outer, binaryEnc);
            binaryEnc.flush();

            writer.write(outer, jsonEnc);
            jsonEnc.flush();
        } catch (Exception e) {
            System.err.println("failed to generate payloads");
            e.printStackTrace(System.err);
            System.exit(1);
        }
    }
}
