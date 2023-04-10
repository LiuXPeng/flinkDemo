package source;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @version 1.0.0
 * @title: Event
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-03-30 18:16
 */


public class Event {
    private static List<String> users = Arrays.asList("aaa", "bbb", "ccc", "dddd", "eeee", "fffff");
    private static List<String> urls = Arrays.asList("www.baidu.com", "www.tencent.com", "www.xiaomi.ocm", "www.bytedace.com");

    private String user;
    private String url;
    private Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public static Event getRandom() {
        Random random = new Random();
        Event event = new Event();
        event.setUrl(urls.get(random.nextInt(urls.size())));
        event.setUser(users.get(random.nextInt(users.size())));
        event.setTimestamp(System.currentTimeMillis());
        return event;
    }
}
