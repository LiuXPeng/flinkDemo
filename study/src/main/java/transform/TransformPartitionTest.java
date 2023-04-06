package transform;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import source.Event;

/**
 * @version 1.0.0
 * @title: TransformPartitionTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-06 21:59
 */


public class TransformPartitionTest {
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

        //随机
//        stream.shuffle().print().setParallelism(4);

        //轮循
//        stream.rebalance().print().setParallelism(4);

        //重缩放
        env.addSource(new RichParallelSourceFunction<Integer>() {
                    @Override
                    public void run(SourceContext<Integer> sourceContext) throws Exception {
                        for (int i = 0; i < 8; i++) {
                            if (i % 2 == getRuntimeContext().getIndexOfThisSubtask()) {
                                sourceContext.collect(i);
                            }
                        }
                    }

                    @Override
                    public void cancel() {

                    }
                }).setParallelism(2)
                .rescale();
//                .print()
//                .setParallelism(4);

//        stream.global().print().setParallelism(4);

        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int i) {
                        return key % 2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print().setParallelism(4);
        env.execute();
    }
}
