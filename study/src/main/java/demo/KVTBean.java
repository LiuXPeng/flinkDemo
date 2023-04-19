package demo;

import java.sql.Timestamp;

/**
 * @version 1.0.0
 * @title: KV
 * @projectName flinkDemo
 * @description: TODO
 * @date： 2023-04-19 19:19
 */


public class KVTBean {
    private String url;
    private Long count;
    private Long end;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "KVTBean{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
