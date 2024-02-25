package com.gtw.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

@Slf4j
public class KafkaConsumerApp {
    public static String BROKERS = "hadoop000:9092";
    public static String TOPIC = "test-1-2";
    public static String GROUP = "group01";

    KafkaConsumer<String, String> consumer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP);

        // 选择分区策略
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, RangeAssignor.class.getName());

        consumer = new KafkaConsumer<>(properties);
    }

    @Test
    public void test01() {
        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Topic:[{}], Partition:[{}], offset:[{}], key:[{}], value[{}]",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 从指定分区消费
     */
    @Test
    public void test02() {
        // 手动指定要消费的分区
        consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Topic:[{}], Partition:[{}], offset:[{}], key:[{}], value[{}]",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    /**
     * 与test03_2同时跑，测试rebalance功能
     */
    @Test
    public void test03_1() {
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("取消分区：{}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("重新分配的分区为：{}", partitions);
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Topic:[{}], Partition:[{}], offset:[{}], key:[{}], value[{}]",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @Test
    public void test03_2() {
        consumer.subscribe(Arrays.asList(TOPIC), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                log.info("取消分区：{}", partitions);
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                log.info("重新分配的分区为：{}", partitions);
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Topic:[{}], Partition:[{}], offset:[{}], key:[{}], value[{}]",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }
    }

    @After
    public void tearDown() {
        if(consumer != null) {
            consumer.close();
        }
    }
}
