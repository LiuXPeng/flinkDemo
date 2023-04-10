package sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import source.Event;

/**
 * @version 1.0.0
 * @title: Sink2Redis
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-10 14:15
 */


public class Sink2Redis {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream = env.fromElements(new Event("jeny", "baidu", 4000L),
                new Event("bob", "tebxb", 5000L),
                new Event("kata", "bb", 6000L),
                new Event("jeny", "badfidu", 10000L),
                new Event("kata", "tebdfxb", 8000L),
                new Event("kata", "bb", 9000L),
                new Event("bob", "baidu", 2000L),
                new Event("jeny", "tebxb", 4000L),
                new Event("bob", "dbb", 4000L),
                new Event("jeny", "baidu", 7000L),
                new Event("bob", "tebxb", 8000L),
                new Event("kata", "bb1", 1000L)
        );

//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
//                .setHost()
//                .build();
//
//        stream.addSink(new RedisSink<>())
    }

}
