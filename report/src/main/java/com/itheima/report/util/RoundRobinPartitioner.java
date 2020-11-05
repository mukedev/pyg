package com.itheima.report.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhangYu
 * @date 2020/10/31
 */
public class RoundRobinPartitioner implements Partitioner {

    // 计数器
    private AtomicInteger counter = new AtomicInteger();


    @Override
    public int partition(String topic, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        //获取分区数量
        Integer partitions = cluster.partitionCountForTopic(topic);
        int currPatition = counter.incrementAndGet() % partitions;
        if (counter.get() > 65535) {
            counter.set(0);
        }
        return currPatition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
