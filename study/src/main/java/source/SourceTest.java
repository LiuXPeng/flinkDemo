package source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @version 1.0.0
 * @title: SourceTest
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-03-30 18:15
 */


public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<String> stream1 = env.readTextFile("/home/liuxiaopeng/IdeaProjects/flinkDemo/study/src/main/resources/clicks.txt");
        stream1.print("1");


        env.execute();
    }
}
