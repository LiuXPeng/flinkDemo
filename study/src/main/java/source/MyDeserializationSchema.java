package source;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pojo.CollectionDoubleData;

import java.io.IOException;
import java.io.Serializable;

/**
 * @version 1.0.0
 * @title: MyDeserializationSchema
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-10 16:16
 */

class MyDeserializationSchema implements DeserializationSchema<String>, Serializable {

    private static Logger log = LoggerFactory.getLogger(MyDeserializationSchema.class);

    private static final long serialVersionUID = 6030161151041195512L;
    public static final String collectionDoubleData="{\n" +
            "    \"name\":\"collectionDoubleData\",\n" +
            "    \"type\":\"record\",\n" +
            "    \"fields\":[\n" +
            "        {\n" +
            "            \"name\":\"devTypeID\",\n" +
            "            \"type\":\"string\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\":\"devID\",\n" +
            "            \"type\":\"string\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\":\"attributeCode\",\n" +
            "            \"type\":\"string\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\":\"origin\",\n" +
            "            \"type\":\"long\",\n" +
            "            \"logicalType\":\"timestamp-millis\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\":\"quality\",\n" +
            "            \"type\":\"string\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\":\"doubleData\",\n" +
            "            \"type\":\"double\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"name\":\"unit\",\n" +
            "            \"type\":\"string\"\n" +
            "        }\n" +
            "    ]\n" +
            "    \n" +
            "}";

//    private DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(collectionDoubleData));


    @Override
    public String deserialize(byte[] bytes) throws IOException {
//        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(collectionDoubleData));
//        Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
//        GenericRecord result = reader.read(null, decoder);
//        log.info(result.get("attributeCode").toString());
        CollectionDoubleData collectionDoubleData = CollectionDoubleData.getBySchema(bytes);
        if (collectionDoubleData != null) {
            return collectionDoubleData.toString();
        }
        return  "attributeCode";
//        return new String(bytes);
    }

    @Override
    public boolean isEndOfStream(String s) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
