package watermark_window;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;
import pojo.UrlViewCount;
import source.Event;

import java.time.Duration;

/**
 * @version 1.0.0
 * @title: LateDataTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-05-04 19:12
 */


public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //并行度不设置为1,运行结果和视频有区别
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
//        DataStreamSource<Event> source = env.addSource(new ClickSource());
        //nc -lk 7777
        SingleOutputStreamOperator<Event> source = env.socketTextStream("localhost", 7777)
                .map(new MapFunction<String, Event>() {
                    @Override
                    public Event map(String s) throws Exception {
                        String[] fields = s.split(",");
                        return new Event(fields[0].trim(), fields[1].trim(), Long.valueOf(fields[2]));
                    }
                });
        SingleOutputStreamOperator<Event> stream1 = source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                })
        );
        stream1.print("input");

        OutputTag<Event> late = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlViewCount> result = stream1.keyBy(data -> data.getUrl())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UrlCountViewExample.UrlViewCountAgg(), new UrlCountViewExample.UrlViewCountResult());

        result.print("result");
        result.getSideOutput(late).print("late");

        env.execute();
    }
}
