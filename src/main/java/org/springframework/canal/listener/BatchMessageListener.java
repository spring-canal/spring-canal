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

package org.springframework.canal.listener;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;

/**
 * @param <T> the fetched records type (the canal entries' generic type)
 * @author 橙子
 * @since 2020/12/8
 */
@FunctionalInterface
public interface BatchMessageListener<T> extends MessageListener<CanalEntries<T>> {
    default void onMessage(Message records, Acknowledgment acknowledgment, CanalConnector connector) {
        throw new UnsupportedOperationException("This batch listener doesn't support records");
    }

    default boolean wantsOriginMessage(Object originData) {
        return false;
    }

    default void setRawEntry(boolean rawEntry) {
    }
}
