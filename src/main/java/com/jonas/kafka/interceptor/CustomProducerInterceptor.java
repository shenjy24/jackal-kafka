package com.jonas.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 自定义生产者拦截器
 *
 * @author shenjy
 * @version 1.0
 * @date 2021-08-29
 */
public class CustomProducerInterceptor implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;

    /**
     * 在消息序列化和计算分区之前调用
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(), record.timestamp(), record.key(), modifiedValue, record.headers());
    }

    /**
     * 在消息被应答之前或消息发送失败时调用，优先于用户设定的Callback之前执行。
     * 该方法运行在Producer的IO线程中，所以逻辑应越简单越好，否则会影响消息的发送性能。
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (null == exception) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendSuccess + sendFailure);
        System.out.printf("发送成功率=%f%n", successRatio);
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
