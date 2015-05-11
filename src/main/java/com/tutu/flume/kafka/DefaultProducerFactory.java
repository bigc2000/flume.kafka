package com.tutu.flume.kafka;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

public class DefaultProducerFactory implements ProducerFactory<byte[], byte[]> {
    private static Map<Integer,Producer<byte[],byte[]>> producerMap = new HashMap<Integer,Producer<byte[],byte[]>>();
    @Override
    public Producer<byte[], byte[]> getProducer(Properties prop) {
        synchronized(producerMap)
        {
            Producer<byte[],byte[]> producer = producerMap.get(prop.hashCode());
            if(producer == null){
                producer = new KafkaProducer<byte[],byte[]>(prop);
            }
            return producer;
        }
    }
}
