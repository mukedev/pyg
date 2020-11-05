package com.itheima.report.bean;

/**
 * @author zhangYu
 * @date 2020/10/31
 */
public class Message {

    /**
     * 消息次数
     */
    private int count;
    /**
     * 消息的时间戳
     */
    private Long timestamp;
    /**
     * 消息体
     */
    private String message;


    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return "Message{" +
                "count=" + count +
                ", timestamp=" + timestamp +
                ", message='" + message + '\'' +
                '}';
    }
}
