package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.Event;

/**
 * @version 1.0.0
 * @title: TransformMapTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-03-31 11:16
 */


public class TransformMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("jeny", "baidu", 4000L),
                new Event("bob", "tebxb", 5000L),
                new Event("kata", "bb", 6000L));
        SingleOutputStreamOperator<String> result1 = stream.map(new MyMapper());
        SingleOutputStreamOperator<String> result2 = stream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getUrl();
            }
        });
        SingleOutputStreamOperator<String> result3 = stream.map(data -> data.getUser());
        result1.print();
        result2.print();
        result3.print();
        env.execute();
    }
}

class MyMapper implements MapFunction<Event, String> {

    @Override
    public String map(Event event) throws Exception {
        return event.getUser();
    }
}

