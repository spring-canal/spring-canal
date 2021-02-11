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

import org.springframework.canal.listener.EachAckConnectorAwareMessageListener;
import org.springframework.canal.listener.EachMessageListener;
import org.springframework.canal.support.Acknowledgment;

/**
 * @param <T> the entry type
 * @author 橙子
 * @since 2020/12/6
 */
public class FilteringEachMessageListenerAdapter<T> extends AbstractFilteringMessageListenerAdapter<EachMessageListener<T>, T> implements EachAckConnectorAwareMessageListener<T> {
    public FilteringEachMessageListenerAdapter(EachMessageListener<T> delegate, MessageFilterStrategy<T> filterStrategy) {
        super(delegate, filterStrategy);
    }

    @Override
    public void onMessage(T data) {
        if (!filter(data))
            getDelegate().onMessage(data);
    }

    @Override
    public void onMessage(T data, CanalConnector connector) {
        if (!filter(data))
            getDelegate().onMessage(data, connector);
    }

    @Override
    public void onMessage(T data, Acknowledgment acknowledgment) {
        if (!filter(data))
            getDelegate().onMessage(data, acknowledgment);
        else if (acknowledgment != null)
            acknowledgment.ack();
    }

    @Override
    public void onMessage(T data, Acknowledgment acknowledgment, CanalConnector connector) {
        if (!filter(data))
            getDelegate().onMessage(data, acknowledgment, connector);
        else if (acknowledgment != null)
            acknowledgment.ack();
    }
}
