/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */
package com.tutu.flume.kafka;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * It is a normal configuration in flume config
 * 
 * <pre>
 * {agent}.sinks.{sink}.maxBatchSize=500
 * {agent}.sinks.{sink}.maxTimeOut=500
 * {agent}.sinks.{sink}.topicHeaderKey=topic
 * #if Event has not config the topic,used this default topic;
 * {agent}.sinks.{sink}.topic=test 
 * #The following configuration with the prefix "kafka" used only for KafkaProducer.
 * {@link #BROKER_LIST}
 * {agent}.sinks.{sink}.kafka.metadata.broker.list =localhost:9092
 * {agent}.sinks.{sink}.kafka.bootstrap.servers=localhost:9092
 * {agent}.sinks.{sink}.kafka.client.id=tailsource
 * {agent}.sinks.{sink}.kafka.key.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
 * {agent}.sinks.{sink}.kafka.value.serializer=org.apache.kafka.common.serialization.ByteArraySerializer
 * {agent}.sinks.{sink}.kafka.serializer.class=kafka.serializer.DefaultEncoder
 * {agent}.sinks.{sink}.kafka.key.serializer.class=kafka.serializer.DefaultEncoder
 * </pre>
 *
 * <pre>
 * <li>metadata.broker.list 
 * <li>request.required.acks 
 * <li>producer.type 
 * <li>serializer.class 
 * Configuration for the Kafka
 * Producer. Documentation for these configurations can be found in the <a
 * href="http://kafka.apache.org/documentation.html#new-producer">Kafka documentation</a>
 * </pre>
 * 
 * @author Free
 *
 */

public class KafkaProducerConf{

    public static final String DEFAULT_EVENT_HEADER_TOPIC_VAL = "topic";
    public static final String DEFAULT_EVENT_HEADER_KAFKA_VAL = "kafka";

    public static final String DEFAULT_MESSAGE_SERIALIZER_CLASS_VAL = "kafka.serializer.DefaultEncoder";
    public static final String DEFAULT_KEY_SERIALIZER_VAL = "kafka.serializer.DefaultEncoder";
    public static final String DEFAULT_KEY_SERIALIZER_CLASS_VAL = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_MESSAGE_SERIALIZER_VAL = "org.apache.kafka.common.serialization.ByteArraySerializer";
    public static final String DEFAULT_REQUIRED_ACKS_VAL = "1";

    Map<String, String> defaultKafkaProcuderConf = new HashMap<String, String>();
    Properties kafkaProducerConf = new Properties();
    
    public KafkaProducerConf() {
        defaultKafkaProcuderConf.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER_VAL);
        defaultKafkaProcuderConf.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_MESSAGE_SERIALIZER_VAL);
        defaultKafkaProcuderConf.put(ProducerConfig.ACKS_CONFIG, DEFAULT_REQUIRED_ACKS_VAL);
        kafkaProducerConf.putAll(defaultKafkaProcuderConf);
    }
    public KafkaProducerConf(Map<String,String> cfg) {
        this();
        kafkaProducerConf.putAll(cfg);
    }
    public KafkaProducerConf(Properties props){
        this();
        kafkaProducerConf.putAll(props);
    }
    /**
     * 
     * @return the current config will used in KafkaProducer
     */
    public Properties getKafkaProduderConf() {
        return kafkaProducerConf;
    }
    /**
     * @return the default kafka producer config
     */    
    public Map<String, String> getDefaultKafkaProduderConf() {
        return Collections.unmodifiableMap(defaultKafkaProcuderConf);
    }
    public String getKafkaProducerConf(String key){
        return kafkaProducerConf.get(key).toString();
    }
}
