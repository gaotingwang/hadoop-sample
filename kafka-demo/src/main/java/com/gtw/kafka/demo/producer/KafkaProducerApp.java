package com.gtw.kafka.demo.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class KafkaProducerApp {

    public static String BROKERS = "hadoop000:9092";
    public static String TOPIC = "test-1-1";

    KafkaProducer<String, String> producer;

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(properties);
    }

    /**
     * 异步发送
     */
    @Test
    public void test01() {
        for(int i = 0; i < 5; i++) {
            producer.send(new ProducerRecord<>(TOPIC, "test content " + i));
        }
    }

    /**
     * 带回调异步发送
     */
    @Test
    public void test02() {
        for(int i = 10; i < 15; i++) {
            producer.send(new ProducerRecord<>(TOPIC, "test content " + i), new Callback() {
                /**
                 * @param recordMetadata  消息元数据信息
                 * @param e 发送失败异常不为空
                 */
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
     * 同步发送,通过返回的Future.get()来获取结果，会产生阻塞
     */
    @Test
    public void test03() throws Exception {
        for(int i = 20; i < 25; i++) {
            producer.send(new ProducerRecord<>(TOPIC, "test content " + i)).get();
        }
    }

    @After
    public void tearDown() {
        if(producer != null) {
            producer.close();
        }
    }
}
