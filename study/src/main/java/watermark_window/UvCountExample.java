package watermark_window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import source.ClickSource;
import source.Event;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @version 1.0.0
 * @title: UvCountExample
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-13 19:15
 */


public class UvCountExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.getConfig().setAutoWatermarkInterval(100);
        DataStreamSource<Event> source = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Event> stream1 = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                })
        );
        stream1.print("data");
        stream1.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UvAgg(), new UvCountResult())
                .print("UV");

        env.execute();
    }

//    public static class UvCountByWindwon extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
//        @Override
//        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
//            HashSet<String> userSet = new HashSet<>();
//            for (Event event : iterable) {
//                userSet.add(event.getUser());
//            }
//            int size = userSet.size();
//            long start = context.window().getStart();
//            long end = context.window().getEnd();
//            collector.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end) + " UV值为" + size);
//        }
//    }

    public static class UvAgg implements AggregateFunction<Event, HashSet<String>, Long>{
        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(Event event, HashSet<String> accumulator) {
            accumulator.add(event.getUser());
            return accumulator;
        }

        @Override
        public Long getResult(HashSet<String> accumulator) {
            return (long) accumulator.size();
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }


    public static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Long, String, Boolean, TimeWindow>.Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            Long uv = iterable.iterator().next();
            collector.collect("窗口 " + new Timestamp(start) + "~" + new Timestamp(end) + " UV值为" + uv);
        }
    }
}
