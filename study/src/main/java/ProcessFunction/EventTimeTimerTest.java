package ProcessFunction;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import source.ClickSource;
import source.Event;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @version 1.0.0
 * @title: EventTimeTimerTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-05-24 19:23
 */


public class EventTimeTimerTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
//        DataStreamSource<Event> source = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.getTimestamp();
                            }
                        }));
        stream.keyBy(data->data.getUser())
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, KeyedProcessFunction<String, Event, String>.Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timestamp();
                        out.collect("数据到达，到达时间：" + new Timestamp(currTs)+",watermark:" + ctx.timerService().currentWatermark());
                        ctx.timerService().registerEventTimeTimer(currTs + 10000);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发，触发时间：" + new Timestamp(timestamp));
                    }
                }).print();

        env.execute();
    }
}
