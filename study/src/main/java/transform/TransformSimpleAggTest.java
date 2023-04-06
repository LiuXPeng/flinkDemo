package transform;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.Event;

/**
 * @version 1.0.0
 * @title: TransformSimpleAggTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-06 11:29
 */


public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
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
//`        stream.keyBy(new KeySelector<Event, String>() {
//            @Override
//            public String getKey(Event event) throws Exception {
//                return event.getUser();
//            }
//        }).max("timestamp")
//                .print("max: ");`

        stream.keyBy(new KeySelector<Event, String>() {
                    @Override
                    public String getKey(Event event) throws Exception {
                        return event.getUser();
                    }
                }).maxBy("timestamp")
                .print("maxBy: ");

        env.execute();
    }
}
