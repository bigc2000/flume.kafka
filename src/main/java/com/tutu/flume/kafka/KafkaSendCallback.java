package com.tutu.flume.kafka;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The call back can do some metric and has more debug info, no Callback in Flume 1.6(not released yet,2015-5-5)
 * so we cannot metric for each send event 
 * 
 * @author Free
 */
public class KafkaSendCallback implements Callback {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaSendCallback.class);

    final AtomicInteger sendCompletedCounter;
    final AtomicBoolean bSendFailed;
    final Lock lock;
    final Condition cond;
    public KafkaSendCallback(final AtomicBoolean bSendFailed, final AtomicInteger sendCompleteCounter, final Lock lock,final Condition condition) {
        this.sendCompletedCounter = sendCompleteCounter;
        this.bSendFailed = bSendFailed;
        this.lock = lock;
        this.cond = condition;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception ex) {
        if (ex == null) {
            if (logger.isDebugEnabled()&&sendCompletedCounter.get()==0) {
                logger.debug(String.format("send all events success,topic:%s,partition:%d,offset:%d", metadata.topic(), metadata.partition(),
                        metadata.offset()));
            }
            sendCompletedCounter.decrementAndGet();
        } else {
            bSendFailed.set(false);
            ex.printStackTrace();
        }

    }
}
