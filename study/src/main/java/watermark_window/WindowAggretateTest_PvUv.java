package watermark_window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import source.ClickSource;
import source.Event;

import java.time.Duration;
import java.util.HashSet;

/**
 * @version 1.0.0
 * @title: WindowAggretateTest_PvUv
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-12 19:30
 */


public class WindowAggretateTest_PvUv {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setAutoWatermarkInterval(100);
        env.setParallelism(1);
        DataStreamSource<Event> stream1 = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> stream2 = stream1.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                })
        );
        stream1.print("data");
        stream2.keyBy(data->true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                .aggregate(new AvgPv())
                .print("res");
        env.execute();
    }

    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double> {
        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> acc) {
            acc.f1.add(event.getUser());
            return Tuple2.of(acc.f0 +1, acc.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            return (double) longHashSetTuple2.f0/longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }
}
