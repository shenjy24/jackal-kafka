package com.jonas.kafka;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class ProducerManager implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ProducerManager.class);

    private int failedTimes = 0;
    private volatile boolean running;
    private final Producer<String, String> producer;
    private final BlockingQueue<ProducerRecord<String, String>> sendQueue = new LinkedBlockingQueue<>();

    private final ExecutorService executorService = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("kafka_send_thread").setDaemon(true).build());

    public ProducerManager() {
        this.producer = new KafkaProducer<>(getProducerProps());
        this.running = true;
        this.executorService.submit(this);
    }

    public void destroy() {
        this.producer.flush();
        this.producer.close();
        this.running = false;
    }

    /**
     * 发送消息，缓存至阻塞队列中
     *
     * @param data 消息
     */
    public void sendMessage(ProducerRecord<String, String> data) {
        if (!running || null == producer) {
            return;
        }
        if (!sendQueue.offer(data)) {
            logger.error("sendMessage failed，the data maybe full!");
        }
    }

    /**
     * 直接发送消息，不经过缓存队列
     *
     * @param data 消息
     */
    public void sendDirectMessage(ProducerRecord<String, String> data) {
        if (!running || null == producer) {
            return;
        }
        try {
            producer.send(data, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    failedTimes = 0;
                    if (e != null) {
                        logger.error("send message error", e);
                    } else {
                        logger.info("send message success");
                    }
                }
            });
        } catch (IllegalStateException e) {
            failedTimes++;
            logger.error("sendDirectMessage failed, so Destroy!", e);
            if (failedTimes > 10) {
                logger.error("sendDirectMessage failed to much times, so close it");
                destroy();
            }
        }
    }

    @Override
    public void run() {
        while (running) {
            try {
                ProducerRecord<String, String> data = sendQueue.take();
                long now = System.currentTimeMillis();
                this.producer.send(data, new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (null != e) {
                            logger.error("send message error", e);
                        } else {
                            logger.info("send message success");
                        }
                        long cEnd = System.currentTimeMillis();
                        logger.debug("---------send end : {}", (cEnd - now));
                    }
                });
            } catch (Throwable throwable) {
                logger.error("ProducerManager run failed!", throwable);
            }
        }
    }

    private Properties getProducerProps() {
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
