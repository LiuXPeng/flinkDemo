package transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.Event;

/**
 * @version 1.0.0
 * @title: TransformReduceTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-06 16:52
 */


public class TransformReduceTest {
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

        SingleOutputStreamOperator<Tuple2<String, Long>> reduce = stream.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.getUser(), event.getTimestamp());
                    }
                }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
                        return Tuple2.of(t1.f0, t1.f1 > t2.f1 ? t1.f1 : t2.f1);
                    }
                });

        reduce.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> t2, Tuple2<String, Long> t1) throws Exception {
                return t1.f1 > t2.f1 ? t1 : t2;
            }
        }).print();

        env.execute();
    }
}
