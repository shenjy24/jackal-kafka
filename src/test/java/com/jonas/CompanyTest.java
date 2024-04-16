package com.jonas;

import com.jonas.kafka.company.CompanyConsumer;
import com.jonas.kafka.company.CompanyProducer;
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
