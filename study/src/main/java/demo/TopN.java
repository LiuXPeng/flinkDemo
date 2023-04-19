package demo;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import source.ClickSource;
import source.Event;

import java.time.Duration;

/**
 * @version 1.0.0
 * @title: TopOne
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-19 17:10
 */


public class TopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> stream1 = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                })
        );
        stream1.print("input");
        stream1.keyBy(data -> data.getUrl())
                .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(5)))
                .aggregate(new UrlCount(), new TopNCount())
                .keyBy(data->data.getEnd())
                .process(new CustomKeyProcess(1))
                .print("max");

        env.execute();
    }


    public static class UrlCount implements AggregateFunction<Event, Tuple2<String, Long>, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> createAccumulator() {
            return Tuple2.of("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(Event event, Tuple2<String, Long> acc) {
            return Tuple2.of(event.getUrl(), acc.f1 + 1);
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> stringLongTuple2) {
            return stringLongTuple2;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> acc1) {
            return null;
        }
    }


    public static class TopNCount extends ProcessWindowFunction<Tuple2<String, Long>, KVTBean, String, TimeWindow> {
        @Override
        public void process(String s, ProcessWindowFunction<Tuple2<String, Long>, KVTBean, String, TimeWindow>.Context context, Iterable<Tuple2<String, Long>> iterable, Collector<KVTBean> collector) throws Exception {
            KVTBean kVTBean = new KVTBean();
            for (Tuple2<String, Long> data : iterable) {
                kVTBean.setCount(data.f1);
                kVTBean.setUrl(data.f0);
            }
            kVTBean.setEnd(context.window().getEnd());
            collector.collect(kVTBean);
        }
    }

    public static class CustomKeyProcess extends KeyedProcessFunction<Long, KVTBean, String> {
        private Integer n;

        ListState<KVTBean> kvtBeanListState;

        public CustomKeyProcess(Integer n) {
            this.n = n;
        }

        @Override
        public void onTimer(long timestamp, KeyedProcessFunction<Long, KVTBean, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
            KVTBean res = null;
            for (KVTBean kvtBean : kvtBeanListState.get()) {
                if (res == null) {
                    res = kvtBean;
                }
                if (kvtBean.getCount() > res.getCount()) {
                    res = kvtBean;
                }
            }
            out.collect(res.toString());
            kvtBeanListState.clear();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            kvtBeanListState = getRuntimeContext().getListState(new ListStateDescriptor<KVTBean>("sdfa", KVTBean.class));
        }

        @Override
        public void processElement(KVTBean kvtBean, KeyedProcessFunction<Long, KVTBean, String>.Context context, Collector<String> collector) throws Exception {
            kvtBeanListState.add(kvtBean);
            context.timerService().registerEventTimeTimer(kvtBean.getEnd() + 1);
        }
    }
}
