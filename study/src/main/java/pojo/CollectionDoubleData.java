package pojo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 1.0.0
 * @title: CollectionDoubleData
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-10 18:48
 */


public class CollectionDoubleData {
    private static final long serialVersionUID = 6030161151041195512L;
    private static Logger log = LoggerFactory.getLogger(CollectionDoubleData.class);
    private String devTypeID;
    private String devID;
    private String attributeCode;
    private long origin;
    private String quality;
    private double doubleData;
    private String unit;

    public String getDevTypeID() {
        return devTypeID;
    }

    public void setDevTypeID(String devTypeID) {
        this.devTypeID = devTypeID;
    }

    public String getDevID() {
        return devID;
    }

    public void setDevID(String devID) {
        this.devID = devID;
    }

    public String getAttributeCode() {
        return attributeCode;
    }

    public void setAttributeCode(String attributeCode) {
        this.attributeCode = attributeCode;
    }

    public long getOrigin() {
        return origin;
    }

    public void setOrigin(long origin) {
        this.origin = origin;
    }

    public String getQuality() {
        return quality;
    }

    public void setQuality(String quality) {
        this.quality = quality;
    }

    public double getDoubleData() {
        return doubleData;
    }

    public void setDoubleData(double doubleData) {
        this.doubleData = doubleData;
    }

    public String getUnit() {
        return unit;
    }

    public void setUnit(String unit) {
        this.unit = unit;
    }

    public static final String s = "{\n" +
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

    public static CollectionDoubleData getBySchema(byte[] bytes) {
        try {
            CollectionDoubleData collectionDoubleData = new CollectionDoubleData();
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(new Schema.Parser().parse(s));
            Decoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
            GenericRecord result = reader.read(null, decoder);
            collectionDoubleData.setDevTypeID(result.get("devTypeID").toString());
            collectionDoubleData.setDevID(result.get("devTypeID").toString());
            collectionDoubleData.setAttributeCode(result.get("attributeCode").toString());
            collectionDoubleData.setOrigin(Long.valueOf( result.get("origin").toString()));
            collectionDoubleData.setQuality(result.get("quality").toString());
            collectionDoubleData.setDoubleData(Double.valueOf(result.get("doubleData").toString()));
            collectionDoubleData.setUnit(result.get("unit").toString());
            return collectionDoubleData;
        } catch (Exception e) {
            log.error("反序列化失败" + e.getStackTrace()[0]);
            return null;
        }
    }

    @Override
    public String toString() {
        return "CollectionDoubleData{" +
                "devTypeID='" + devTypeID + '\'' +
                ", devID='" + devID + '\'' +
                ", attributeCode='" + attributeCode + '\'' +
                ", origin=" + origin +
                ", quality='" + quality + '\'' +
                ", doubleData=" + doubleData +
                ", unit='" + unit + '\'' +
                '}';
    }
}
