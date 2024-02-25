package com.gtw.kafka.demo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class KafkaOffsetApp {
    public static String BROKERS = "hadoop000:9092";
    public static String TOPIC = "test-1-2";
    public static String GROUP = "group02";

    KafkaConsumer<String, String> consumer;
    Properties properties;

    @Before
    public void setUp() {
        properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
    }

    /**
     * 自动提交offset
     */
    @Test
    public void test01() {
        // 为true，周期性自动提交
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        // 自动提交的间隔
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        consumer = new KafkaConsumer<>(properties);

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
     * 手动提交offset
     */
    @Test
    public void test02() {
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList(TOPIC));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                log.info("Topic:[{}], Partition:[{}], offset:[{}], key:[{}], value[{}]",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
            // 同步提交，阻塞，重试机制，效率低
            consumer.commitSync();
            // 异步提交，非阻塞，无重试机制，效率高
//            consumer.commitAsync();
        }
    }

    @After
    public void tearDown() {
        if(consumer != null) {
            consumer.close();
        }
    }
}
