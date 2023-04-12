package watermark_window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import source.ClickSource;
import source.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @version 1.0.0
 * @title: WindowAggretateTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-12 19:15
 */


public class WindowAggretateTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> stream1 = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> stream2 = stream1.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                })
        );

        stream2.keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<Event, Tuple2<Long, Integer>,String>() {
                    @Override
                    public Tuple2<Long, Integer> createAccumulator() {
                        return Tuple2.of(0L, 0);
                    }

                    @Override
                    public Tuple2<Long, Integer> add(Event event, Tuple2<Long, Integer> acc) {
                        return Tuple2.of(acc.f0 + event.getTimestamp(), acc.f1 + 1);
                    }

                    @Override
                    public String getResult(Tuple2<Long, Integer> longIntegerTuple2) {
                        Timestamp timestamp = new Timestamp(longIntegerTuple2.f0 / longIntegerTuple2.f1);
                        return timestamp.toString();
                    }

                    @Override
                    public Tuple2<Long, Integer> merge(Tuple2<Long, Integer> v1, Tuple2<Long, Integer> v2) {
                        return Tuple2.of(v1.f0+v2.f0, v1.f1 + v2.f1);
                    }
                })
                        .print();

        env.execute();

    }
}
