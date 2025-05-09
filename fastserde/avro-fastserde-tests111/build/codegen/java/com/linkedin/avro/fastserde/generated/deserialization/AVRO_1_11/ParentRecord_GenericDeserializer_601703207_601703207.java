
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_11;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import com.linkedin.avro.fastserde.customized.DatumReaderCustomization;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class ParentRecord_GenericDeserializer_601703207_601703207
    implements FastDeserializer<IndexedRecord>
{

    private final Schema readerSchema;
    private final Schema class_field0;
    private final Schema if_field0;
    private final Schema public_field0;

    public ParentRecord_GenericDeserializer_601703207_601703207(Schema readerSchema) {
        this.readerSchema = readerSchema;
        this.class_field0 = readerSchema.getField("class_field").schema();
        this.if_field0 = readerSchema.getField("if_field").schema();
        this.public_field0 = readerSchema.getField("public_field").schema();
    }

    public IndexedRecord deserialize(IndexedRecord reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        return deserializeParentRecord0((reuse), (decoder), (customization));
    }

    public IndexedRecord deserializeParentRecord0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord parentRecord0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == readerSchema)) {
            parentRecord0 = ((IndexedRecord)(reuse));
        } else {
            parentRecord0 = new org.apache.avro.generic.GenericData.Record(readerSchema);
        }
        parentRecord0 .put(0, deserializeclass0(parentRecord0 .get(0), (decoder), (customization)));
        populate_ParentRecord0((parentRecord0), (customization), (decoder));
        return parentRecord0;
    }

    public IndexedRecord deserializeclass0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord class0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == class_field0)) {
            class0 = ((IndexedRecord)(reuse));
        } else {
            class0 = new org.apache.avro.generic.GenericData.Record(class_field0);
        }
        Utf8 charSequence0;
        Object oldString0 = class0 .get(0);
        if (oldString0 instanceof Utf8) {
            charSequence0 = (decoder).readString(((Utf8) oldString0));
        } else {
            charSequence0 = (decoder).readString(null);
        }
        class0 .put(0, charSequence0);
        return class0;
    }

    private void populate_ParentRecord0(IndexedRecord parentRecord0, DatumReaderCustomization customization, Decoder decoder)
        throws IOException
    {
        parentRecord0 .put(1, deserializeif0(parentRecord0 .get(1), (decoder), (customization)));
        parentRecord0 .put(2, deserializepublic0(parentRecord0 .get(2), (decoder), (customization)));
    }

    public IndexedRecord deserializeif0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord if0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == if_field0)) {
            if0 = ((IndexedRecord)(reuse));
        } else {
            if0 = new org.apache.avro.generic.GenericData.Record(if_field0);
        }
        Utf8 charSequence1;
        Object oldString1 = if0 .get(0);
        if (oldString1 instanceof Utf8) {
            charSequence1 = (decoder).readString(((Utf8) oldString1));
        } else {
            charSequence1 = (decoder).readString(null);
        }
        if0 .put(0, charSequence1);
        return if0;
    }

    public IndexedRecord deserializepublic0(Object reuse, Decoder decoder, DatumReaderCustomization customization)
        throws IOException
    {
        IndexedRecord public0;
        if ((((reuse)!= null)&&((reuse) instanceof IndexedRecord))&&(((IndexedRecord)(reuse)).getSchema() == public_field0)) {
            public0 = ((IndexedRecord)(reuse));
        } else {
            public0 = new org.apache.avro.generic.GenericData.Record(public_field0);
        }
        Utf8 charSequence2;
        Object oldString2 = public0 .get(0);
        if (oldString2 instanceof Utf8) {
            charSequence2 = (decoder).readString(((Utf8) oldString2));
        } else {
            charSequence2 = (decoder).readString(null);
        }
        public0 .put(0, charSequence2);
        return public0;
    }

}
