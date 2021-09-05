package com.jonas.kafka;

import com.jonas.kafka.pojo.Company;
import com.jonas.kafka.serializer.CompanySerializer;
import com.jonas.kafka.serializer.ProtostuffSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * CompanyProducer
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-09-05
 */
public class CompanyProducer {

    public void sendMessage() {
        //Producer是线程安全的
        KafkaProducer<String, Company> producer = new KafkaProducer<>(config());
        for (int i = 0; i < 100; i++) {
            //send方法是异步的，将记录放入缓冲池中并立即返回
            producer.send(new ProducerRecord<>("topicA", Integer.toString(i), new Company("Jonas-" + i, "China")));
        }
        producer.close();
    }

    private Properties config() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("linger.ms", 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ProtostuffSerializer.class.getName());
        return props;
    }
}
