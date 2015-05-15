/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.tutu.flume.kafka;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.annotations.InterfaceStability.Evolving;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.AbstractSink;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Evolving
public class KafkaSink extends AbstractSink implements Configurable {
    private static final String CHARSET_KEY = "key.charset";
    private static final Logger logger = LoggerFactory.getLogger(KafkaSink.class);
    public static final String KAFKA_CONFIG_PREFIX = "kafka.";
    // used in sink config only
    public static final String MAX_BATCH_SIZE_KEY = "maxBatchSize";
    public static final String SEND_TIME_OUT_KEY = "sendTimeoutMs";
    // used in Event Header
    public static final String EVENT_HEADER_TOPIC_KEY = "header.topickey";
    public static final String EVENT_HEADER_KAFKA_KEY = "header.key";

    // default value
    public static final int DEFAULT_BATCH_SIZE_VAL = 100;

    // if both topic and topic in even header no set,use the default topic
    public static final String DEFAULT_TOPIC_VAL = "test";
    public static final String TOPIC_KEY = "topic";

    public static final int DEFAULT_SEND_TIME_OUT_VAL = 3000;

    private KafkaSinkCounter sinkCounter;
    // sink config
    private int maxBatchSize;
    private long maxSendTimeoutMs;
    Producer<byte[], byte[]> producer;
    KafkaProducerConf kafkaSinkConf;
    String defaultTopic;
    String eventTopicHeader;
    String eventKeyHeader;
    String charset;
    AtomicInteger sendCompletedCount = new AtomicInteger();
    // ArrayList<ProducerRecord<byte[], byte[]>> messageList;

    //
    final Lock lock = new ReentrantLock();
    final Condition condition = lock.newCondition();

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction tx = channel.getTransaction();
        Status status = Status.READY;
        long eventStartTime = System.currentTimeMillis();
        boolean shouldCommit = true;
        try {

            tx.begin();
            sendCompletedCount.set(0);
            AtomicBoolean sendFailed = new AtomicBoolean(false);
            int eventCount = 0;
            String keyStr = null;
            byte[] key = null;
            for (int i = 0; i < maxBatchSize; ++i) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }
                // send event to kakfa producer buffer
                eventCount++;
                String topic = defaultTopic;
                if (event.getHeaders() != null) {
                    keyStr = event.getHeaders().get(eventKeyHeader);
                    if (keyStr == null) {
                        key = null;
                    } else {
                        key = keyStr.getBytes(charset);
                    }
                    String eventTopic = event.getHeaders().get(eventTopicHeader);
                    if (eventTopic != null && !eventTopic.trim().isEmpty()) {
                        topic = defaultTopic;
                    }
                }

                ProducerRecord<byte[], byte[]> message = new ProducerRecord<byte[], byte[]>(topic, key, event.getBody());
                /**
                 * !IMPORTANT,there are two choices: send message here one bye one or add to list and than batch send,
                 * because take event from channel also time-costing task,especially when the batch size is big. Unlike
                 * flume 1.6 default, here I used send one-by-one orther than batch send. It is not a good way to use
                 * Future, because the Futuer.get() is a blocking method, and we need Futures in Guava.
                 */
                if (sendFailed.get()) {
                    break;
                }
                producer.send(message, new KafkaSendCallback(sendFailed, sendCompletedCount, lock, condition,i));
                // messageList.add(message);
            }
            long sendStartTime = 0;
            long sendEndTime = 0;
            if (eventCount == 0) {
                sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;
                shouldCommit = true;
            } else {
                sendStartTime = System.currentTimeMillis();
                // add metrics,how about use Metrics of Kafka client
                sinkCounter.addToBatchEventTakeTimer(sendStartTime - eventStartTime);
                // record send time cost
                if (logger.isDebugEnabled()) {
                    logger.debug("Take {} events (include part of sending) cost {} ms", eventCount, System.currentTimeMillis() - eventStartTime);
                }
                if (eventCount < maxBatchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(eventCount);
                // add kafa metric
                // wait callback return
                while ((sendCompletedCount.get() < eventCount) && !sendFailed.get()) {
                    try {
                        // wait.If each event send exceeded the maxSendTimeoutMs,failed.
                        lock.lock();
                        if (!condition.await(maxSendTimeoutMs, TimeUnit.MILLISECONDS)) {
                            sendFailed.set(true);
                            sinkCounter.incrementKafkaSendTimeoutCount();
                            logger.warn(String.format("eventcount:%d,sendcount:%d,timeout:%d ms", eventCount,
                                    sendCompletedCount.get(), (int) maxSendTimeoutMs));
                        }
                    } catch (InterruptedException ex) {
                        Thread.sleep(0);
                    } catch (Exception ex) {
                        logger.error("Condition.await fail, {}", ex);
                    } finally {
                        lock.unlock();
                    }
                }
                if (sendFailed.get()) {
                    shouldCommit = false;
                    // this metrics is not actually event count sent, because after send() called,
                    // we cannot cancel send task, and doesnot know exactly success count.
                    sinkCounter.addToEventDrainSuccessCount(sendCompletedCount.get());
                } else {
                    sinkCounter.addToEventDrainSuccessCount(eventCount);
                }
                sendEndTime = System.currentTimeMillis();
                sinkCounter.addToKafkaEventSendTimer(sendEndTime - sendStartTime);
            }
            if (shouldCommit) {
                tx.commit();
            } else {
                tx.rollback();
                sinkCounter.incrementRollbackCount();
            }
            // log
            if (logger.isDebugEnabled() && eventCount > 0) {
                if (shouldCommit) 
                {
                    logger.debug("Send {} events successful, cost {} ms", eventCount, sendEndTime - sendStartTime);
                } else 
                {
                    logger.debug("Send {} events failed, cost at least {} ms", eventCount,
                            System.currentTimeMillis() - sendStartTime);
                }
            }
        } catch (Throwable t) {
            tx.rollback();
            if (t instanceof Error) {
                throw (Error) t;
            } else if (t instanceof ChannelException) {
                logger.error(getName() + " " + this + ", Unable to take event from channel " + channel.getName()
                        + ". Exception folows." + t);
                status = Status.BACKOFF;
            } else {
                throw new EventDeliveryException("Failed to send events", t);
            }
        } finally {
            tx.close();
        }
        return status;
    }

    @Override
    public void configure(final Context context) {
        Configurables.ensureRequiredNonNull(context, KAFKA_CONFIG_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        Properties kafkaProducerProps = new Properties();
        kafkaProducerProps.putAll(context.getSubProperties(KAFKA_CONFIG_PREFIX));
        kafkaSinkConf = new KafkaProducerConf(kafkaProducerProps);
        if (sinkCounter == null) {
            sinkCounter = new KafkaSinkCounter(getName());
        }
        // Flume Sink Config
        charset = context.getString(CHARSET_KEY, "UTF-8");
        maxBatchSize = context.getInteger(MAX_BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE_VAL);
        if (maxBatchSize < 1) {
            logger.warn("INVALID!, {} must greater than 0, use defalut {}", MAX_BATCH_SIZE_KEY, DEFAULT_BATCH_SIZE_VAL);
            maxBatchSize = DEFAULT_BATCH_SIZE_VAL;
        }
        maxSendTimeoutMs = context.getInteger(SEND_TIME_OUT_KEY, DEFAULT_SEND_TIME_OUT_VAL);
        if (maxSendTimeoutMs < 0) {
            logger.warn("INVALID! {} must greater than 0,use default.{}", SEND_TIME_OUT_KEY, SEND_TIME_OUT_KEY);
            maxSendTimeoutMs = DEFAULT_SEND_TIME_OUT_VAL;
        }
        defaultTopic = context.getString(TOPIC_KEY, DEFAULT_TOPIC_VAL);
        // messageList = new ArrayList<ProducerRecord<byte[], byte[]>>(maxBatchSize);
        if (logger.isDebugEnabled()) {
            logger.debug(formatConf(context, kafkaProducerProps));
        }
        logger.info("Sink Config: maxBatchSize: " + maxBatchSize + ", maxTimeoutMs: " + maxSendTimeoutMs);
    }

    @Override
    public synchronized void start() {
        logger.info("Starting {} {}", this, getName());
        if (producer != null) {
            producer.close();
        }
        DefaultProducerFactory defaultProducerFactory = new DefaultProducerFactory();
        producer = defaultProducerFactory.getProducer(kafkaSinkConf.getKafkaProduderConf());
        sinkCounter.start();
        super.start();
        logger.info("Started {} {}", this, getName());
    }

    @Override
    public synchronized void stop() {
        logger.info("Stoping {} {}", this, getName());
        sinkCounter.stop();
        super.stop();
        logger.info("Stopped {} {}", this, getName());
    }

    private static String formatConf(Context ctx, Map<?, ?> kafkaProducerConfMap) {
        StringBuilder sb = new StringBuilder();
        sb.append("SinkConf:").append(System.lineSeparator());
        final Map<String, String> map = ctx.getParameters();
        for (Entry<String, String> e : map.entrySet()) {
            String key = e.getKey();
            String val = e.getValue();
            if (!key.startsWith(KAFKA_CONFIG_PREFIX)) {
                sb.append(key).append(":").append(val).append(System.lineSeparator());
            }
        }
        sb.append("KafakProducer:").append(System.lineSeparator());
        for (Entry<?, ?> e : kafkaProducerConfMap.entrySet()) {
            String key = e.getKey().toString();
            String val = e.getValue().toString();
            sb.append(key).append(":").append(val).append(System.lineSeparator());
        }
        return sb.substring(0, sb.length() - 1);
    }
}
