package source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Scanner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @version 1.0.0
 * @title: KeyboardSource
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-03-30 19:30
 */


public class KeyboardSource implements SourceFunction<String> {
    private AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (running.get()){
            sourceContext.collect(scanner.nextLine());
        }
    }

    @Override
    public void cancel() {
        running.set(false);
    }
}
