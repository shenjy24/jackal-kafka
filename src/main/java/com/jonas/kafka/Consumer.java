package com.jonas.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class Consumer {

    /**
     * 按照分区粒度同步提交消费位移
     */
    public void partitionConsume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config("false"));
        consumer.subscribe(Collections.singletonList("topicA"));
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        //业务逻辑处理
                        process(record);
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastConsumedOffset + 1)));
                }
            }
        } finally {
            consumer.close();
        }
    }

    /**
     * 如果poll返回的数据过多，可以分批次进行提交
     */
    public void batchConsume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config("false"));
        consumer.subscribe(Collections.singletonList("topicA"));
        int count = 0;
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    process(record);
                    offsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                    //每处理 100 条消息就提交一次位移
                    if (count++ % 100 == 0) {
                        consumer.commitAsync(offsets, null);
                    }
                }
            }
        } catch (Exception e) {
            //处理异常
        } finally {
            try {
                //最后一次提交使用同步阻塞式提交
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 最佳实践：
     * 1. 对于常规性、阶段性的手动提交，我们调用 commitAsync() 避免程序阻塞；
     * 2. 在 Consumer 要关闭前，我们调用 commitSync() 方法执行同步阻塞式的位移提交，以确保 Consumer 关闭前能够保存正确的位移数据。
     */
    public void consume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config("false"));
        consumer.subscribe(Collections.singletonList("topicA"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    //业务逻辑完成后再提交偏移量
                    process(buffer);
                    //commitAsync 是不会重试的，使用异步提交规避阻塞
                    consumer.commitAsync();
                    buffer.clear();
                }
            }
        } catch (Exception e) {
            //处理异常
        } finally {
            try {
                //最后一次提交使用同步阻塞式提交
                consumer.commitSync();
            } finally {
                consumer.close();
            }
        }
    }

    /**
     * 手动提交消费偏移量：使用commitSync同步提交，会阻塞消费端
     */
    public void manualCommitConsume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config("false"));
        consumer.subscribe(Collections.singletonList("topicA"));
        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                buffer.add(record);
            }
            if (buffer.size() >= minBatchSize) {
                //业务逻辑完成后再提交偏移量
                process(buffer);
                consumer.commitSync();
                buffer.clear();
            }
        }
    }

    /**
     * 手动提交消费位移：每消费一条消息提交一次位移
     */
    public void manualCommitOneByOneConsume() {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config("false"));
        consumer.subscribe(Collections.singletonList("topicA"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                //业务逻辑处理
                process(record);
                //位移提交
                TopicPartition partition = new TopicPartition(record.topic(), record.partition());
                OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(record.offset() + 1);
                consumer.commitSync(Collections.singletonMap(partition, offsetAndMetadata));
            }
        }
    }

    /**
     * 自动提交消费偏移量
     */
    public void autoCommitConsume() {
        //Consumer不是线程安全的
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(config("true"));
        consumer.subscribe(Collections.singletonList("topicA"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    private Properties config(String autoCommit) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", autoCommit);
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    private void process(ConsumerRecord<String, String> record) {
        System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
    }

    private void process(List<ConsumerRecord<String, String>> records) {
        for (ConsumerRecord<String, String> record : records) {
            process(record);
        }
    }
}
