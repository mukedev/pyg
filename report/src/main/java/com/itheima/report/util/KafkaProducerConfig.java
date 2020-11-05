package com.itheima.report.util;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import sun.security.krb5.Config;

import java.util.HashMap;
import java.util.Map;

/**
 * // @Configurable表示此类为配置类
 *
 * @author zhangYu
 * @date 2020/10/31
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${kafka.bootstrap_servers_config}")
    private String bootstrap_servers_config;
    @Value("${kafka.retries_config}")
    private String retries_config;
    @Value("${kafka.batch_size_config}")
    private String batch_size_config;
    @Value("${kafka.linger_ms_config}")
    private String linger_ms_config;
    @Value("${kafka.buffer_memory_config}")
    private String buffer_memory_config;

    @Bean
    public KafkaTemplate kafkaTemplate() {
        // 构建工厂所需要的配置
        Map<String, Object> configs = new HashMap<>();

        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers_config);
        configs.put(ProducerConfig.RETRIES_CONFIG, retries_config);
        configs.put(ProducerConfig.BATCH_SIZE_CONFIG, batch_size_config);
        configs.put(ProducerConfig.LINGER_MS_CONFIG, linger_ms_config);
        configs.put(ProducerConfig.BUFFER_MEMORY_CONFIG, buffer_memory_config);
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,RoundRobinPartitioner.class);
        //设置key的序列化器
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        // 创建生产者工厂
        ProducerFactory producerFactory = new DefaultKafkaProducerFactory(configs);
        // 返回一个KafkaTemplate 对象
        return new KafkaTemplate<String, String>(producerFactory);
    }
}
