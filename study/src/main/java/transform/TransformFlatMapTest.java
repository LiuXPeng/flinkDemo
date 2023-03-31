
package transform;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.collection.parallel.ParIterableLike;
import source.Event;

/**
 * @title: TransformFlatMapTest
 * @projectName flinkDemo
 * @description: TODO
 * @date：  2023-03-31 19:26
 * @version 1.0.0
 */
 
 
public class TransformFlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("jeny", "baidu", 4000L),
                new Event("bob", "tebxb", 5000L),
                new Event("kata", "bb", 6000L));

        stream.flatMap(new MyFlatMap()).print("1");
        stream.flatMap((Event event, Collector<String> collector) -> {
            if (event.getUrl().length()>3) {
                collector.collect(event.getUrl());
            }else {
                collector.collect(event.getUser());
            }
        }).returns(new TypeHint<String>() {})
                .print("2");
        env.execute();

    }
}

class MyFlatMap implements FlatMapFunction<Event, String> {
    @Override
    public void flatMap(Event event, Collector<String> collector) throws Exception {
        if (event.getUrl().length()>3) {
            collector.collect(event.getUrl());
        }else {
            collector.collect(event.getUser());
        }
    }
}
