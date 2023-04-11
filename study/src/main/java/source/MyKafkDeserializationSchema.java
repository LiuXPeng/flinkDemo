package source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pojo.CollectionDoubleData;

/**
 * @version 1.0.0
 * @title: MyKafkDeserializationSchema
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-11 11:18
 */


public class MyKafkDeserializationSchema implements KafkaDeserializationSchema<String> {
    private static Logger log = LoggerFactory.getLogger(MyKafkDeserializationSchema.class);

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        String key =new String(record.key());
        log.warn("key:{}", key);

        return CollectionDoubleData.getBySchema(record.value()).getAttributeCode();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return null;
    }
}
