/*
 * Copyright 2020-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.springframework.canal.listener.adapter;

import com.alibaba.otter.canal.client.CanalConnector;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;
import org.springframework.messaging.Message;

import java.util.List;

/**
 * @param <T> the entity type
 * @author 橙子
 * @since 2020/12/13
 */
@FunctionalInterface
public interface BatchToEachAdapter<T> {
    /**
     * Adapt the list and invoke the callback for each message.
     *
     * @param messages  the messages.
     * @param entries   the records.
     * @param ack       the acknowledgment.
     * @param connector the consumer.
     * @param callback  the callback.
     */
    void adapt(List<Message<?>> messages, CanalEntries<T> entries, Acknowledgment ack, CanalConnector connector, EachCallback<T> callback);

    @FunctionalInterface
    interface EachCallback<T> {
        /**
         * Handle each message.
         *
         * @param message        the message.
         * @param entry          the record.
         * @param acknowledgment the acknowledgment.
         * @param connector      the consumer.
         */
        void invoke(Message<?> message, T entry, Acknowledgment acknowledgment, CanalConnector connector);
    }
}
