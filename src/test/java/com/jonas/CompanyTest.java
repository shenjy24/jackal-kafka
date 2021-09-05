package com.jonas;

import com.jonas.kafka.CompanyConsumer;
import com.jonas.kafka.CompanyProducer;
import com.jonas.kafka.Consumer;
import com.jonas.kafka.Producer;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class CompanyTest {

    private CompanyProducer producer;
    private CompanyConsumer consumer;

    @Before
    public void init() {
        producer = new CompanyProducer();
        consumer = new CompanyConsumer();
    }

    @Test
    public void testSend() {
        producer.sendMessage();
    }

    @Test
    public void testConsume() {
        consumer.consume();
    }
}
