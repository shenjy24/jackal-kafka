package com.jonas.kafka;

import com.jonas.kafka.deserializer.CompanyDeserializer;
import com.jonas.kafka.deserializer.ProtostuffDeserializer;
import com.jonas.kafka.pojo.Company;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * CompanyConsumer
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-09-05
 */
public class CompanyConsumer {

    /**
     * 自动提交消费偏移量
     */
    public void consume() {
        //Consumer不是线程安全的
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(config());
        consumer.subscribe(Collections.singletonList("topicA"));
        while (true) {
            ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Company> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    private Properties config() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ProtostuffDeserializer.class.getName());
        return props;
    }
}
