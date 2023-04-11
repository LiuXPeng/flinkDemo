package source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.Properties;

/**
 * @version 1.0.0
 * @title: KafkaSourceTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-10 15:25
 */


public class KafkaSourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置信息
        Properties prop = new Properties();
        //zk 地址
        prop.setProperty("bootstrap.servers", "192.168.1.12:49092");
        //消费者组
        prop.setProperty("group.id", "consumer-groupdfs");
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        prop.setProperty("auto.offset.reset", "latest");

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer =
                new FlinkKafkaConsumer<String>("realdata", new MyKafkDeserializationSchema(), prop);

        //自动提交 offset
        stringFlinkKafkaConsumer.setCommitOffsetsOnCheckpoints(true);

        env.addSource(stringFlinkKafkaConsumer).returns(Types.STRING).print();

        env.execute();
    }

}

//class MyDeserializationSchema implements DeserializationSchema<String> {
//    @Override
//    public String deserialize(byte[] bytes) throws IOException {
//
//        return new String(bytes);
//    }
//
//    @Override
//    public boolean isEndOfStream(String s) {
//        return false;
//    }
//
//    @Override
//    public TypeInformation<String> getProducedType() {
//        return null;
//    }
//}
