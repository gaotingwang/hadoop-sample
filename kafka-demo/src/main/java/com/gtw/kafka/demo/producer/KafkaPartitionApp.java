package com.gtw.kafka.demo.producer;

import com.gtw.kafka.demo.partitioner.MyPartitioner;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

/**
 * 分区策略测试
 */
@Slf4j
public class KafkaPartitionApp {

    public static String BROKERS = "hadoop000:9092";
    public static String TOPIC = "test-1-2";

    KafkaProducer<String, String> producer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 指定分区器
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, MyPartitioner.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    /**
     * 指定分区
     */
    @Test
    public void test01() {
        int partitionId = 1;
        for(int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(TOPIC, partitionId, "", "test content " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        log.info("Topic is {}, partition is {}", recordMetadata.topic(), recordMetadata.partition());
                    }else {
                        log.error("{} topic send error", recordMetadata.topic(), e);
                    }
                }
            });
        }
    }

    /**
     * hash(key) % partition数量
     * 不指定分区和key，选择粘性分区器，随机选，分区达到阈值，选择另一个分区
     */
    @Test
    public void test02() {
        for(int i = 10; i < 15; i++) {
            producer.send(new ProducerRecord<>(TOPIC, i + "", "test content " + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        log.info("Topic is {}, partition is {}", recordMetadata.topic(), recordMetadata.partition());
                    }else {
                        log.error("{} topic send error", recordMetadata.topic(), e);
                    }
                }
            });
        }
    }

    /**
     * 使用自定义分区器
     */
    @Test
    public void test03() {
        for(int i = 20; i < 25; i++) {
            String content;
            if(i % 2 == 0) {
                content = "study content " + i;
            }else {
                content = "test content " + i;
            }
            producer.send(new ProducerRecord<>(TOPIC, i + "", content), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null) {
                        log.info("Topic is {}, partition is {}", recordMetadata.topic(), recordMetadata.partition());
                    }else {
                        log.error("{} topic send error", recordMetadata.topic(), e);
                    }
                }
            });
        }
    }

    @After
    public void tearDown() {
        if(producer != null) {
            producer.close();
        }
    }
}
