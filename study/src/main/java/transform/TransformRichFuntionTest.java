package transform;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.Event;

/**
 * @version 1.0.0
 * @title: TransformRichFuntionTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-06 21:23
 */


public class TransformRichFuntionTest {
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

        stream.map(new MyRichMapper()).print();

        env.execute();
    }
}

class MyRichMapper extends RichMapFunction<Event, Integer> {
    @Override
    public Integer map(Event event) throws Exception {
        return event.getUrl().length();
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("open生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务启动");
    }

    @Override
    public void close() throws Exception {
        super.close();
        System.out.println("close生命周期被调用" + getRuntimeContext().getIndexOfThisSubtask() + "号任务结束");
    }
}