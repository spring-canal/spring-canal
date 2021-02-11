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
import com.alibaba.otter.canal.protocol.Message;

import org.springframework.canal.listener.BatchAckConnectorAwareMessageListener;
import org.springframework.canal.listener.BatchMessageListener;
import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;

/**
 * @param <T> the entry type
 * @author 橙子
 * @since 2020/12/6
 */
public class FilteringBatchMessageListenerAdapter<T> extends AbstractFilteringMessageListenerAdapter<BatchMessageListener<T>, T> implements BatchAckConnectorAwareMessageListener<T> {
    private final boolean ignoreEmpty;

    public FilteringBatchMessageListenerAdapter(BatchMessageListener<T> delegate, MessageFilterStrategy<T> filterStrategy) {
        this(delegate, filterStrategy, true);
    }

    public FilteringBatchMessageListenerAdapter(BatchMessageListener<T> delegate, MessageFilterStrategy<T> filterStrategy, boolean ignoreEmpty) {
        super(delegate, filterStrategy);
        this.ignoreEmpty = ignoreEmpty;
    }

    @Override
    public void onMessage(CanalEntries<T> data) {
        data.removeIf(this::filter);
        if (!data.isEmpty() || !this.ignoreEmpty)
            getDelegate().onMessage(data);
    }

    @Override
    public void onMessage(CanalEntries<T> data, CanalConnector connector) {
        data.removeIf(this::filter);
        if (!data.isEmpty() || !this.ignoreEmpty)
            getDelegate().onMessage(data, connector);
    }

    @Override
    public void onMessage(CanalEntries<T> data, Acknowledgment acknowledgment) {
        data.removeIf(this::filter);
        if (!data.isEmpty() || !this.ignoreEmpty)
            getDelegate().onMessage(data, acknowledgment);
        else if (acknowledgment != null)
            acknowledgment.ack();
    }

    @Override
    public void onMessage(CanalEntries<T> data, Acknowledgment acknowledgment, CanalConnector connector) {
        data.removeIf(this::filter);
        if (!data.isEmpty() || !this.ignoreEmpty)
            getDelegate().onMessage(data, acknowledgment, connector);
        else if (acknowledgment != null)
            acknowledgment.ack();
    }

    @Override
    public void onMessage(Message records, Acknowledgment acknowledgment, CanalConnector connector) {
        getDelegate().onMessage(records, acknowledgment, connector);
    }

    @Override
    public void setRawEntry(boolean rawEntry) {
        getDelegate().setRawEntry(rawEntry);
    }
}
