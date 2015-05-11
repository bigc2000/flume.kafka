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

import com.tutu.flume.ExtendableSinkCounter;

public class KafkaSinkCounter extends ExtendableSinkCounter implements KafkaSinkCounterMBean {

    private static final String BATCH_EVENT_TAKE_TIME = "channel.event.take.time";
    private static final String EVENT_SEND_TIME = "kafka.send.time";
    private static final String SEND_TIMEOUT_COUNT = "kafka.send.timeout";
    private static final String ROLLBACK_COUNT = "kafak.rollback.count";

    public KafkaSinkCounter(String name) {
        super(name, EVENT_SEND_TIME, SEND_TIMEOUT_COUNT, ROLLBACK_COUNT, BATCH_EVENT_TAKE_TIME);
    }

    public KafkaSinkCounter(String name, String... args) {
        super(name, args);
    }

    public long addToBatchEventTakeTimer(long delta) {
        return addAndGet(BATCH_EVENT_TAKE_TIME, delta);
    }

    public long addToKafkaEventSendTimer(long delta) {
        return addAndGet(EVENT_SEND_TIME, delta);
    }

    public long incrementKafkaSendTimeoutCount() {
        return increment(SEND_TIMEOUT_COUNT);
    }

    public long incrementRollbackCount() {
        return increment(ROLLBACK_COUNT);
    }

    public long getBatchEventTakeTimer() {
        return get(BATCH_EVENT_TAKE_TIME);
    }

    public long getKafkaEventSendTimer() {
        return get(EVENT_SEND_TIME);
    }
    public long getSendTimeoutCount(){
        return get(SEND_TIMEOUT_COUNT);
    }
    public long getRollbackCount() {
        return get(ROLLBACK_COUNT);
    }

}