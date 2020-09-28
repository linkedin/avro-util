
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class ClasspathTestRecord_SpecificDeserializer_4580844570251215039_2703298965059642600
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord>
{

    private final Schema readerSchema;

    public ClasspathTestRecord_SpecificDeserializer_4580844570251215039_2703298965059642600(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord deserialize(com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeClasspathTestRecord0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord deserializeClasspathTestRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord ClasspathTestRecord;
        if ((reuse)!= null) {
            ClasspathTestRecord = ((com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord)(reuse));
        } else {
            ClasspathTestRecord = new com.linkedin.avro.fastserde.generated.avro.ClasspathTestRecord();
        }
        Object oldString0 = ClasspathTestRecord.get(0);
        if (oldString0 instanceof Utf8) {
            ClasspathTestRecord.put(0, (decoder).readString(((Utf8) oldString0)));
        } else {
            ClasspathTestRecord.put(0, (decoder).readString(null));
        }
        decoder.skipFixed(1);
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.skipFixed(1);
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'classpathFixedUnion': "+ unionIndex0));
        }
        long chunkLen0 = (decoder.readArrayStart());
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object classpathFixedArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    classpathFixedArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                decoder.skipFixed(1);
            }
            chunkLen0 = (decoder.arrayNext());
        }
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    decoder.skipFixed(1);
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        }
        long chunkLen2 = (decoder.readArrayStart());
        while (chunkLen2 > 0) {
            for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                Object classpathFixedUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    classpathFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                switch (unionIndex1) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        decoder.skipFixed(1);
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'classpathFixedUnionArrayElem': "+ unionIndex1));
                }
            }
            chunkLen2 = (decoder.arrayNext());
        }
        long chunkLen3 = (decoder.readMapStart());
        if (chunkLen3 > 0) {
            do {
                for (int counter3 = 0; (counter3 <chunkLen3); counter3 ++) {
                    Utf8 key1 = (decoder.readString(null));
                    int unionIndex2 = (decoder.readIndex());
                    switch (unionIndex2) {
                        case  0 :
                            decoder.readNull();
                            break;
                        case  1 :
                            decoder.skipFixed(1);
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'classpathFixedUnionMapValue': "+ unionIndex2));
                    }
                }
                chunkLen3 = (decoder.mapNext());
            } while (chunkLen3 > 0);
        }
        decoder.readEnum();
        int unionIndex3 = (decoder.readIndex());
        switch (unionIndex3) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.readEnum();
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'classpathEnumUnion': "+ unionIndex3));
        }
        long chunkLen4 = (decoder.readArrayStart());
        while (chunkLen4 > 0) {
            for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
                decoder.readEnum();
            }
            chunkLen4 = (decoder.arrayNext());
        }
        long chunkLen5 = (decoder.readMapStart());
        if (chunkLen5 > 0) {
            do {
                for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                    Utf8 key2 = (decoder.readString(null));
                    decoder.readEnum();
                }
                chunkLen5 = (decoder.mapNext());
            } while (chunkLen5 > 0);
        }
        long chunkLen6 = (decoder.readArrayStart());
        while (chunkLen6 > 0) {
            for (int counter6 = 0; (counter6 <chunkLen6); counter6 ++) {
                Object classpathEnumUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    classpathEnumUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex4 = (decoder.readIndex());
                switch (unionIndex4) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        decoder.readEnum();
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'classpathEnumUnionArrayElem': "+ unionIndex4));
                }
            }
            chunkLen6 = (decoder.arrayNext());
        }
        long chunkLen7 = (decoder.readMapStart());
        if (chunkLen7 > 0) {
            do {
                for (int counter7 = 0; (counter7 <chunkLen7); counter7 ++) {
                    Utf8 key3 = (decoder.readString(null));
                    int unionIndex5 = (decoder.readIndex());
                    switch (unionIndex5) {
                        case  0 :
                            decoder.readNull();
                            break;
                        case  1 :
                            decoder.readEnum();
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'classpathEnumUnionMapValue': "+ unionIndex5));
                    }
                }
                chunkLen7 = (decoder.mapNext());
            } while (chunkLen7 > 0);
        }
        deserializeClasspathSubRecord0(null, (decoder));
        int unionIndex6 = (decoder.readIndex());
        switch (unionIndex6) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                deserializeClasspathSubRecord0(null, (decoder));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'classpathSubRecordUnion': "+ unionIndex6));
        }
        long chunkLen8 = (decoder.readArrayStart());
        while (chunkLen8 > 0) {
            for (int counter8 = 0; (counter8 <chunkLen8); counter8 ++) {
                Object classpathSubRecordArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    classpathSubRecordArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                deserializeClasspathSubRecord0(classpathSubRecordArrayArrayElementReuseVar0, (decoder));
            }
            chunkLen8 = (decoder.arrayNext());
        }
        long chunkLen9 = (decoder.readArrayStart());
        while (chunkLen9 > 0) {
            for (int counter9 = 0; (counter9 <chunkLen9); counter9 ++) {
                Object classpathSubRecordUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    classpathSubRecordUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex7 = (decoder.readIndex());
                switch (unionIndex7) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        deserializeClasspathSubRecord0(classpathSubRecordUnionArrayArrayElementReuseVar0, (decoder));
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'classpathSubRecordUnionArrayElem': "+ unionIndex7));
                }
            }
            chunkLen9 = (decoder.arrayNext());
        }
        long chunkLen10 = (decoder.readMapStart());
        if (chunkLen10 > 0) {
            do {
                for (int counter10 = 0; (counter10 <chunkLen10); counter10 ++) {
                    Utf8 key4 = (decoder.readString(null));
                    deserializeClasspathSubRecord0(null, (decoder));
                }
                chunkLen10 = (decoder.mapNext());
            } while (chunkLen10 > 0);
        }
        long chunkLen11 = (decoder.readMapStart());
        if (chunkLen11 > 0) {
            do {
                for (int counter11 = 0; (counter11 <chunkLen11); counter11 ++) {
                    Utf8 key5 = (decoder.readString(null));
                    int unionIndex8 = (decoder.readIndex());
                    switch (unionIndex8) {
                        case  0 :
                            decoder.readNull();
                            break;
                        case  1 :
                            deserializeClasspathSubRecord0(null, (decoder));
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'classpathSubRecordUnionMapValue': "+ unionIndex8));
                    }
                }
                chunkLen11 = (decoder.mapNext());
            } while (chunkLen11 > 0);
        }
        return ClasspathTestRecord;
    }

    public void deserializeClasspathSubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
    }

}
