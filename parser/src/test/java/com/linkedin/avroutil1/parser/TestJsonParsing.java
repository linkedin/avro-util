/*
 * Copyright 2021 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.parser;

import com.linkedin.avroutil1.parser.jsonpext.JsonObjectExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonReaderExt;
import com.linkedin.avroutil1.parser.jsonpext.JsonReaderWithLocations;
import com.linkedin.avroutil1.parser.jsonpext.JsonValueExt;
import com.linkedin.avroutil1.testcommon.TestUtil;
import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.stream.JsonLocation;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.StringReader;

public class TestJsonParsing {

    @Test
    public void testSimpleSchema() throws Exception {
        String avsc = TestUtil.load("schemas/TestRecord.avsc");
        JsonReader referenceReader = Json.createReader(new StringReader(avsc));
        JsonObject referenceObject = referenceReader.readObject();
        JsonReaderExt ourReader = new JsonReaderWithLocations(new StringReader(avsc), null);
        JsonObjectExt ourObject = ourReader.readObject();

        //assert we read everything correctly. our equals() implementations are not
        //commutative with regular json-p objects (yet?)
        //noinspection SimplifiableAssertion
        Assert.assertTrue(referenceObject.equals(ourObject));

        //see that we have text locations for json values
        JsonValueExt fields = ourObject.get("fields");
        JsonLocation startLocation = fields.getStartLocation();
        JsonLocation endLocation = fields.getEndLocation();
        Assert.assertEquals(startLocation.getLineNumber(), 6);
        Assert.assertEquals(endLocation.getLineNumber(), 49);
    }
}
