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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.apache.flume.annotations.InterfaceStability.Unstable;
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
@Unstable
public class KafkaSendCallback implements Callback {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaSendCallback.class);

    final AtomicInteger sendCompletedCounter;
    final AtomicBoolean bSendFailed;
    final Lock lock;
    final Condition cond;
    final int index;//the index of the record
    public KafkaSendCallback(final AtomicBoolean bSendFailed, final AtomicInteger sendCompleteCounter, final Lock lock,final Condition condition,final int idx) {
        this.sendCompletedCounter = sendCompleteCounter;
        this.bSendFailed = bSendFailed;
        this.lock = lock;
        this.cond = condition;
        this.index= idx;
    }
    public KafkaSendCallback(final AtomicBoolean bSendFailed, final AtomicInteger sendCompleteCounter, final Lock lock,final Condition condition) {
        this(bSendFailed,sendCompleteCounter,lock,condition,0);
    }
    @Override
    public void onCompletion(RecordMetadata metadata, Exception ex) {
        if (ex == null) {
            sendCompletedCounter.incrementAndGet();
        } else {
            logger.debug("send event {} failed.",index);
            bSendFailed.set(false);
            ex.printStackTrace();
        }
    }
}
