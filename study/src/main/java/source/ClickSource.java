package source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @version 1.0.0
 * @title: ClickSource
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-10 19:23
 */


public class ClickSource implements SourceFunction<Event> {
    private AtomicBoolean flag = new AtomicBoolean(true);
    private static List<String> users = Arrays.asList("aaa", "bbb", "ccc", "dddd", "eeee", "fffff");
    private static List<String> urls = Arrays.asList("www.baidu.com", "www.tencent.com", "www.xiaomi.ocm", "www.bytedace.com");


    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        while (flag.get()) {
            sourceContext.collect(Event.getRandom());
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        flag.set(false);
    }
}
