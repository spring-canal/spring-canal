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

package org.springframework.canal.listener.container;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

import org.springframework.canal.support.Acknowledgment;

/**
 * If batch listener wants origin data, listener container task will directly invoking
 * method {@link org.springframework.canal.listener.BatchMessageListener#onMessage(Message, Acknowledgment, CanalConnector)}
 * without create collection of elements,
 * so the generic parameter type is origin data type {@link Message}
 * instead of {@link java.util.Collection Collection&lt;T>}
 * ({@link java.util.Collection<com.alibaba.otter.canal.protocol.CanalEntry.Entry> Collection&lt;CanalEntry.Entry>}
 * or {@link java.util.Collection<com.google.protobuf.ByteString> Collection&lt;ByteString>}).
 *
 * @author 橙子
 * @since 2020/11/17
 */
public interface CanalListenerContainerBatchErrorHandler extends CanalListenerContainerErrorHandler<Message> {
    /**
     * Handle the exception.
     *
     * @param thrownException the exception.
     * @param data            the records.
     *                        <strong>DO NOT ACK WITHIN METHOD, LET METHOD {@link #isAckAfterExceptionHandle()} RETURN TRUE IF NEED.</strong>
     * @param consumer        the consumer.
     * @param container       the container.
     */
    default void handle(Exception thrownException, Message data, CanalConnector consumer, MessageListenerContainer container) {
        handle(thrownException, data);
    }

    /**
     * Handle the exception.
     *
     * @param thrownException the exception.
     * @param data            the records.
     *                        <strong>DO NOT ACK WITHIN METHOD, LET METHOD {@link #isAckAfterExceptionHandle()} RETURN TRUE IF NEED.</strong>
     * @param consumer        the consumer.
     * @param container       the container.
     * @param invokeListener  a callback to re-invoke the listener.
     */
    default void handle(Exception thrownException, Message data, CanalConnector consumer, MessageListenerContainer container, Runnable invokeListener) {
        handle(thrownException, data);
    }
}
