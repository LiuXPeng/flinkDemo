package transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import source.Event;

/**
 * @version 1.0.0
 * @title: TransformFilterTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-03-31 15:28
 */


public class TransformFilterTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(new Event("jeny", "baidu", 4000L),
                new Event("bob", "tebxb", 5000L),
                new Event("kata", "bb", 6000L));
        SingleOutputStreamOperator<Event> filter1 = stream.filter(new MyFilter());
        SingleOutputStreamOperator<Event> filter2 = stream.filter(data -> data.getUrl().length() > 3);


        filter1.print("1");
        filter2.print("2");
        env.execute();
    }


}

class MyFilter implements FilterFunction<Event> {
    @Override
    public boolean filter(Event event) throws Exception {
        return (event.getUrl().length() > 3);
    }
}