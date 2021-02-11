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

import org.springframework.canal.config.ContainerProperties;
import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.canal.listener.MessageListener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Creates one or more {@link DefaultMessageListenerContainer}s based on
 * {@link #setConcurrency(int) concurrency}.
 *
 * @param <T> the default listener container's generic type
 * @author 橙子
 * @since 2020/11/15
 */
public class ConcurrencyMessageListenerContainer<T> extends AbstractMessageListenerContainer {
    private final List<DefaultMessageListenerContainer<T>> containers = new ArrayList<>();
    private int concurrency;

    public ConcurrencyMessageListenerContainer(CanalConnectorFactory connectorFactory, ContainerProperties containerProperties,
                                               MessageListener<?> listener, Collection<String> subscribes, Properties supersedeProperties) {
        super(connectorFactory, containerProperties, listener, subscribes, supersedeProperties);
        this.concurrency = this.containerProperties.getConcurrency() == null ? 1 : this.containerProperties.getConcurrency();
    }

    public void setConcurrency(int concurrency) {
        this.concurrency = concurrency;
    }

    @Override
    public void start() {
        if (this.running)
            return;
        this.running = true;
        for (int i = 0; i < this.concurrency; i++) {
            final DefaultMessageListenerContainer<T> container = new DefaultMessageListenerContainer<>(this, this.connectorFactory, this.containerProperties, this.listener, this.subscribes, this.supersedeProperties);
            container.setBeanName(getBeanName() == null ? "listener" : getBeanName() + '-' + i);
            container.setAutoStartup(isAutoStartup());
            container.setErrorHandler(getErrorHandler());
            container.setTransactionManager(getTransactionManager());
            container.setTransactionDefinition(getTransactionDefinition());
            container.setMonitorScheduler(getMonitorScheduler());
            container.setListenerTaskExecutor(getListenerTaskExecutor());
            if (getApplicationEventPublisher() != null)
                container.setApplicationEventPublisher(getApplicationEventPublisher());
//            container.setAfterRollbackProcessor(getAfterRollbackProcessor())
            container.start();
            this.containers.add(container);
        }
    }

    @Override
    public void stop(boolean wait) {
        this.running = false;
        for (DefaultMessageListenerContainer<T> container : this.containers)
            container.stop(wait);
    }
}
