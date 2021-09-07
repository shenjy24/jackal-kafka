package com.jonas.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * 在Kafka的topic：ods_user中有一些用户数据，数据格式如下：
 * <p>
 * 姓名,性别,出生日期
 * 张三,1,1980-10-09
 * 李四,0,1985-11-01
 * <p>
 * 我们需要编写程序，将用户的性别转换为男、女（1-男，0-女），转换后将数据写入到topic：dwd_user中。
 * 要求使用事务保障，要么消费了数据同时写入数据到 topic，提交offset。要么全部失败。
 */
public class TransactionHandler {

    /**
     * 只有写的场景
     */
    public void handleWrite() {
        KafkaProducer<String, String> producer = createProducer();

        // 初始化事务
        producer.initTransactions();

        try {
            // 开启事务
            producer.beginTransaction();
            // 发送消息到ods_user
            producer.send(new ProducerRecord<>("ods_user","张三,1,1980-10-09"));
            // 发送消息到ods_user
            producer.send(new ProducerRecord<>("ods_user","李四,0,1985-11-01"));
            // 提交事务
            producer.commitTransaction();
        } catch (Exception e) {
            e.printStackTrace();
            // 终止事务
            producer.abortTransaction();
        } finally {
            producer.close();
        }
    }

    /**
     * 消费-生产并存（consume-Transform-Produce）
     */
    public void handleConsumeTransformProduce() {
        KafkaProducer<String, String> producer = createProducer();
        KafkaConsumer<String, String> consumer = createConsumer();

        // 初始化事务
        producer.initTransactions();

        try {
            while (true) {
                // 1. 开启事务
                producer.beginTransaction();
                // 2. 定义Map结构，用于保存分区对应的offset
                Map<TopicPartition, OffsetAndMetadata> offsetCommits = new HashMap<>();
                // 3. 拉取消息
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    // 4. 保存偏移量
                    offsetCommits.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    // 5. 进行转换处理
                    String[] fields = record.value().split(",");
                    fields[1] = fields[1].equalsIgnoreCase("1") ? "男" : "女";
                    String message = fields[0] + "," + fields[1] + "," + fields[2];
                    // 6. 生产消息到dwd_user
                    producer.send(new ProducerRecord<>("dwd_user", message));
                }
                // 7. 提交偏移量到事务
                producer.sendOffsetsToTransaction(offsetCommits, "ods_user");
                // 8. 提交事务
                producer.commitTransaction();
            }
        } catch (Exception e) {
            e.printStackTrace();
            // 终止事务
            producer.abortTransaction();
        } finally {
            producer.close();
            consumer.close();
        }
    }

    private KafkaProducer<String, String> createProducer() {
        // 1. 创建生产者配置
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("transactional.id", "dwd_user");         //生产者需要配置事务ID
        props.put("enable.idempotence", true);             //设置幂等性
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 2. 创建生产者
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer() {
        // 1. 创建Kafka消费者配置
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "ods_user");
        props.put("isolation.level", "read_committed");      //事务级别
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 2. 创建Kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 3. 订阅要消费的主题
        consumer.subscribe(Collections.singletonList("ods_user"));

        return consumer;
    }
}
