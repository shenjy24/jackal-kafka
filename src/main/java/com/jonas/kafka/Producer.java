package com.jonas.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    public void sendMessage() {
        //Producer是线程安全的
        KafkaProducer<String, String> producer = new KafkaProducer<>(config());
        for (int i = 0; i < 100; i++) {
            //send方法是异步的，将记录放入缓冲池中并立即返回
            producer.send(new ProducerRecord<>("topicA", Integer.toString(i), Integer.toString(i)));
        }
        producer.close();
    }

    private Properties config() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }
}
