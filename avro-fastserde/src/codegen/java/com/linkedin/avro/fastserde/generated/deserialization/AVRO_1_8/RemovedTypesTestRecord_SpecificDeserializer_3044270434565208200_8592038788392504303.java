
package com.linkedin.avro.fastserde.generated.deserialization.AVRO_1_8;

import java.io.IOException;
import com.linkedin.avro.fastserde.FastDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.io.Decoder;
import org.apache.avro.util.Utf8;

public class RemovedTypesTestRecord_SpecificDeserializer_3044270434565208200_8592038788392504303
    implements FastDeserializer<com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord>
{

    private final Schema readerSchema;

    public RemovedTypesTestRecord_SpecificDeserializer_3044270434565208200_8592038788392504303(Schema readerSchema) {
        this.readerSchema = readerSchema;
    }

    public com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord deserialize(com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord reuse, Decoder decoder)
        throws IOException
    {
        return deserializeRemovedTypesTestRecord0((reuse), (decoder));
    }

    public com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord deserializeRemovedTypesTestRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord RemovedTypesTestRecord;
        if ((reuse)!= null) {
            RemovedTypesTestRecord = ((com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord)(reuse));
        } else {
            RemovedTypesTestRecord = new com.linkedin.avro.fastserde.generated.avro.RemovedTypesTestRecord();
        }
        Object oldString0 = RemovedTypesTestRecord.get(0);
        if (oldString0 instanceof Utf8) {
            RemovedTypesTestRecord.put(0, (decoder).readString(((Utf8) oldString0)));
        } else {
            RemovedTypesTestRecord.put(0, (decoder).readString(null));
        }
        decoder.skipBytes();
        long chunkLen0 = (decoder.readArrayStart());
        while (chunkLen0 > 0) {
            for (int counter0 = 0; (counter0 <chunkLen0); counter0 ++) {
                Object removedBytesArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedBytesArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                decoder.skipBytes();
            }
            chunkLen0 = (decoder.arrayNext());
        }
        int unionIndex0 = (decoder.readIndex());
        switch (unionIndex0) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.skipBytes();
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'removedBytesUnion': "+ unionIndex0));
        }
        long chunkLen1 = (decoder.readMapStart());
        if (chunkLen1 > 0) {
            do {
                for (int counter1 = 0; (counter1 <chunkLen1); counter1 ++) {
                    Utf8 key0 = (decoder.readString(null));
                    decoder.skipBytes();
                }
                chunkLen1 = (decoder.mapNext());
            } while (chunkLen1 > 0);
        }
        long chunkLen2 = (decoder.readArrayStart());
        while (chunkLen2 > 0) {
            for (int counter2 = 0; (counter2 <chunkLen2); counter2 ++) {
                Object removedBytesUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedBytesUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex1 = (decoder.readIndex());
                switch (unionIndex1) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        decoder.skipBytes();
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'removedBytesUnionArrayElem': "+ unionIndex1));
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
                            decoder.skipBytes();
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'removedBytesUnionMapValue': "+ unionIndex2));
                    }
                }
                chunkLen3 = (decoder.mapNext());
            } while (chunkLen3 > 0);
        }
        decoder.skipFixed(1);
        int unionIndex3 = (decoder.readIndex());
        switch (unionIndex3) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.skipFixed(1);
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'removedFixedUnion': "+ unionIndex3));
        }
        long chunkLen4 = (decoder.readArrayStart());
        while (chunkLen4 > 0) {
            for (int counter4 = 0; (counter4 <chunkLen4); counter4 ++) {
                Object removedFixedArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedFixedArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                decoder.skipFixed(1);
            }
            chunkLen4 = (decoder.arrayNext());
        }
        long chunkLen5 = (decoder.readMapStart());
        if (chunkLen5 > 0) {
            do {
                for (int counter5 = 0; (counter5 <chunkLen5); counter5 ++) {
                    Utf8 key2 = (decoder.readString(null));
                    decoder.skipFixed(1);
                }
                chunkLen5 = (decoder.mapNext());
            } while (chunkLen5 > 0);
        }
        long chunkLen6 = (decoder.readArrayStart());
        while (chunkLen6 > 0) {
            for (int counter6 = 0; (counter6 <chunkLen6); counter6 ++) {
                Object removedFixedUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedFixedUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex4 = (decoder.readIndex());
                switch (unionIndex4) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        decoder.skipFixed(1);
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'removedFixedUnionArrayElem': "+ unionIndex4));
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
                            decoder.skipFixed(1);
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'removedFixedUnionMapValue': "+ unionIndex5));
                    }
                }
                chunkLen7 = (decoder.mapNext());
            } while (chunkLen7 > 0);
        }
        decoder.readEnum();
        int unionIndex6 = (decoder.readIndex());
        switch (unionIndex6) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                decoder.readEnum();
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'removedEnumUnion': "+ unionIndex6));
        }
        long chunkLen8 = (decoder.readArrayStart());
        while (chunkLen8 > 0) {
            for (int counter8 = 0; (counter8 <chunkLen8); counter8 ++) {
                decoder.readEnum();
            }
            chunkLen8 = (decoder.arrayNext());
        }
        long chunkLen9 = (decoder.readMapStart());
        if (chunkLen9 > 0) {
            do {
                for (int counter9 = 0; (counter9 <chunkLen9); counter9 ++) {
                    Utf8 key4 = (decoder.readString(null));
                    decoder.readEnum();
                }
                chunkLen9 = (decoder.mapNext());
            } while (chunkLen9 > 0);
        }
        long chunkLen10 = (decoder.readArrayStart());
        while (chunkLen10 > 0) {
            for (int counter10 = 0; (counter10 <chunkLen10); counter10 ++) {
                Object removedEnumUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedEnumUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex7 = (decoder.readIndex());
                switch (unionIndex7) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        decoder.readEnum();
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'removedEnumUnionArrayElem': "+ unionIndex7));
                }
            }
            chunkLen10 = (decoder.arrayNext());
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
                            decoder.readEnum();
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'removedEnumUnionMapValue': "+ unionIndex8));
                    }
                }
                chunkLen11 = (decoder.mapNext());
            } while (chunkLen11 > 0);
        }
        deserializeRemovedSubRecord0(null, (decoder));
        int unionIndex9 = (decoder.readIndex());
        switch (unionIndex9) {
            case  0 :
                decoder.readNull();
                break;
            case  1 :
                deserializeRemovedSubRecord0(null, (decoder));
                break;
            default:
                throw new RuntimeException(("Illegal union index for 'removedSubRecordUnion': "+ unionIndex9));
        }
        long chunkLen12 = (decoder.readArrayStart());
        while (chunkLen12 > 0) {
            for (int counter12 = 0; (counter12 <chunkLen12); counter12 ++) {
                Object removedSubRecordArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedSubRecordArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                deserializeRemovedSubRecord0(removedSubRecordArrayArrayElementReuseVar0, (decoder));
            }
            chunkLen12 = (decoder.arrayNext());
        }
        long chunkLen13 = (decoder.readArrayStart());
        while (chunkLen13 > 0) {
            for (int counter13 = 0; (counter13 <chunkLen13); counter13 ++) {
                Object removedSubRecordUnionArrayArrayElementReuseVar0 = null;
                if (null instanceof GenericArray) {
                    removedSubRecordUnionArrayArrayElementReuseVar0 = ((GenericArray) null).peek();
                }
                int unionIndex10 = (decoder.readIndex());
                switch (unionIndex10) {
                    case  0 :
                        decoder.readNull();
                        break;
                    case  1 :
                        deserializeRemovedSubRecord0(removedSubRecordUnionArrayArrayElementReuseVar0, (decoder));
                        break;
                    default:
                        throw new RuntimeException(("Illegal union index for 'removedSubRecordUnionArrayElem': "+ unionIndex10));
                }
            }
            chunkLen13 = (decoder.arrayNext());
        }
        long chunkLen14 = (decoder.readMapStart());
        if (chunkLen14 > 0) {
            do {
                for (int counter14 = 0; (counter14 <chunkLen14); counter14 ++) {
                    Utf8 key6 = (decoder.readString(null));
                    deserializeRemovedSubRecord0(null, (decoder));
                }
                chunkLen14 = (decoder.mapNext());
            } while (chunkLen14 > 0);
        }
        long chunkLen15 = (decoder.readMapStart());
        if (chunkLen15 > 0) {
            do {
                for (int counter15 = 0; (counter15 <chunkLen15); counter15 ++) {
                    Utf8 key7 = (decoder.readString(null));
                    int unionIndex11 = (decoder.readIndex());
                    switch (unionIndex11) {
                        case  0 :
                            decoder.readNull();
                            break;
                        case  1 :
                            deserializeRemovedSubRecord0(null, (decoder));
                            break;
                        default:
                            throw new RuntimeException(("Illegal union index for 'removedSubRecordUnionMapValue': "+ unionIndex11));
                    }
                }
                chunkLen15 = (decoder.mapNext());
            } while (chunkLen15 > 0);
        }
        return RemovedTypesTestRecord;
    }

    public void deserializeRemovedSubRecord0(Object reuse, Decoder decoder)
        throws IOException
    {
        decoder.skipString();
    }

}
