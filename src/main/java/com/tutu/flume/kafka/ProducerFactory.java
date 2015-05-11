package com.tutu.flume.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;

public interface ProducerFactory<K,V> {

    public Producer<K,V> getProducer(Properties prop);
}
