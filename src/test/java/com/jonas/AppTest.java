package com.jonas;

import com.jonas.kafka.Consumer;
import com.jonas.kafka.Producer;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest {

    private Producer producer;
    private Consumer consumer;

    @Before
    public void init() {
        producer = new Producer();
        consumer = new Consumer();
    }

    @Test
    public void testSend() {
        producer.sendMessage();
    }

    @Test
    public void testAutoCommitConsume() {
        consumer.autoCommitConsume();
    }
}
