package com.jonas.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class CustomerManager implements Runnable {
    private KafkaConsumer<String, String> consumer;
    private volatile boolean running = false;
    private Set<String> topics;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("kafka_consumer_thread").setDaemon(true).build());

    public CustomerManager(Set<String> topics) {
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(getConsumerProps());
        this.consumer.subscribe(this.topics, new ConsumerRebalanceListener() {
            /**
             * 该方法会在再均衡开始之前和消费者停止读取消息之后被调用
             * 可以通过该方法来处理消费位移的提交
             *
             * @param partitions 再均衡前所分配到的分区
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync(currentOffsets);
                currentOffsets.clear();
            }

            /**
             * 该方法会在重新分配分区之后和消费者开始读取消息之前被调用
             *
             * @param partitions 再均衡后所分配到的分区
             */
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                //获取消费偏移量，实现原理是向协调者发送获取请求
                Map<TopicPartition, OffsetAndMetadata> offsetAndMetadataMap = consumer.committed(new HashSet<>(partitions));
                for (TopicPartition partition : partitions) {
                    OffsetAndMetadata offset = offsetAndMetadataMap.get(partition);
                    if (null != offset) {
                        //设置本地拉取分量，下次拉取消息以这个偏移量为准
                        consumer.seek(partition, offset.offset());
                    }
                }
            }
        });
        this.running = true;
        this.executorService.submit(this);
    }

    @Override
    public void run() {
        while (this.running) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()), new OffsetAndMetadata(record.offset() + 1));
                }
                consumer.commitAsync(currentOffsets, null);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void destroy() {
        this.consumer.close();
        this.running = false;
    }

    private Properties getConsumerProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }
}
