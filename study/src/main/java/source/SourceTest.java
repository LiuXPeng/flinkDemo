package source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

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

        DataStreamSource<String> stream1 = env.readTextFile("study/src/main/resources/clicks.txt");


        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("jeny", "baidu", 1000L));
        events.add(new Event("bob", "tebxb", 2000L));
        events.add(new Event("kata", "bb", 3000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("jeny", "baidu", 4000L),
                new Event("bob", "tebxb", 5000L),
                new Event("kata", "bb", 6000L)
        );


        // nc -lk 7777
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.1.12:49092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserilizer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        DataStreamSource<String> stream5 = env.addSource(new FlinkKafkaConsumer<String>("clicks", new SimpleStringSchema(), properties));


        DataStreamSource<String> stream6 = env.addSource(new KeyboardSource());

        DataStreamSource<Integer> stream7 = env.addSource(new ParallelSourceFunction<Integer>() {
            private AtomicBoolean running = new AtomicBoolean(true);
            private Random random = new Random();

            @Override
            public void run(SourceContext<Integer> sourceContext) throws Exception {
                while (running.get()) {
                    sourceContext.collect(random.nextInt());
                    Thread.sleep(2000);
                }
            }

            @Override
            public void cancel() {
                running.set(false);
            }
        }).setParallelism(6);


//        stream1.print("read file");
//        stream2.print("collect");
//        stream3.print("elements");
//        stream4.print("socket");
//        stream5.print("kafka");
//        stream6.print("keyboard");
//        stream7.print("ParallelSourceFunction");


        env.execute();
    }
}
