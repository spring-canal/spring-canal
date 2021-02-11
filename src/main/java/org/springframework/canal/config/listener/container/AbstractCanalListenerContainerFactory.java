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

package org.springframework.canal.config.listener.container;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.canal.config.ContainerProperties;
import org.springframework.canal.config.endpoint.AbstractCanalListenerEndpoint;
import org.springframework.canal.config.endpoint.CanalListenerEndpoint;
import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.canal.listener.adapter.BatchToEachAdapter;
import org.springframework.canal.listener.adapter.MessageFilterStrategy;
import org.springframework.canal.listener.container.AbstractMessageListenerContainer;
import org.springframework.canal.listener.container.CanalListenerContainerBatchErrorHandler;
import org.springframework.canal.listener.container.CanalListenerContainerEachErrorHandler;
import org.springframework.canal.listener.container.CanalListenerContainerErrorHandler;
import org.springframework.canal.support.converter.record.CanalMessageConverter;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionDefinition;
import org.springframework.util.Assert;

/**
 * @param <T> the listener container type
 * @author 橙子
 * @since 2020/11/17
 */
public abstract class AbstractCanalListenerContainerFactory<T extends AbstractMessageListenerContainer> implements CanalListenerContainerFactory<T>, InitializingBean {
    protected CanalConnectorFactory connectorFactory;
    protected ContainerProperties containerProperties;
    protected CanalMessageConverter messageConverter;
    protected BatchToEachAdapter<?> batchToEachAdapter;
    protected MessageFilterStrategy<?> messageFilterStrategy;
    private boolean defaultBatchListener;
    private boolean defaultAutoStartup = true;
    private CanalListenerContainerErrorHandler<?> errorHandler;
    private PlatformTransactionManager transactionManager;
    private TransactionDefinition transactionDefinition = new DefaultTransactionDefinition();
    private TaskScheduler monitorScheduler;
    private AsyncTaskExecutor listenerTaskExecutor;

    public void setConnectorFactory(CanalConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    public void setContainerProperties(ContainerProperties containerProperties) {
        this.containerProperties = containerProperties;
    }

    public void setMessageConverter(CanalMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    public void setBatchToEachAdapter(BatchToEachAdapter<?> batchToEachAdapter) {
        this.batchToEachAdapter = batchToEachAdapter;
    }

    public void setMessageFilterStrategy(MessageFilterStrategy<?> messageFilterStrategy) {
        this.messageFilterStrategy = messageFilterStrategy;
    }

    public void setDefaultAutoStartup(boolean defaultAutoStartup) {
        this.defaultAutoStartup = defaultAutoStartup;
    }

    public void setDefaultBatchListener(boolean defaultBatchListener) {
        this.defaultBatchListener = defaultBatchListener;
    }

    /**
     * Set the error handler to invoking when the listener throws an exception.
     *
     * @param errorHandler the error handler.
     */
    public void setErrorHandler(CanalListenerContainerErrorHandler<?> errorHandler) {
        this.errorHandler = errorHandler;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public void setTransactionDefinition(TransactionDefinition transactionDefinition) {
        this.transactionDefinition = transactionDefinition;
    }

    public void setMonitorScheduler(TaskScheduler monitorScheduler) {
        this.monitorScheduler = monitorScheduler;
    }

    public void setListenerTaskExecutor(AsyncTaskExecutor listenerTaskExecutor) {
        this.listenerTaskExecutor = listenerTaskExecutor;
    }

    @Override
    public void afterPropertiesSet() {
        if (this.errorHandler == null)
            return;
        if (this.defaultBatchListener)
            Assert.state(this.errorHandler instanceof CanalListenerContainerBatchErrorHandler,
                    () -> "The error handler must be a CanalListenerContainerBatchErrorHandler, not " + this.errorHandler.getClass().getName());
        else
            Assert.state(this.errorHandler instanceof CanalListenerContainerEachErrorHandler,
                    () -> "The error handler must be an CanalListenerContainerEachErrorHandler, not " + this.errorHandler.getClass().getName());
    }

    @Override
    public T createListenerContainer(CanalListenerEndpoint endpoint) {
        if (endpoint instanceof AbstractCanalListenerEndpoint)
            configAbstractListenerEndpoint(endpoint);
        final T listenerContainer = createListenerContainerInstance(endpoint);
        listenerContainer.setAutoStartup(endpoint.getAutoStartup());
        if (this.errorHandler != null)
            listenerContainer.setErrorHandler(this.errorHandler);
        if (this.transactionManager != null)
            listenerContainer.setTransactionManager(this.transactionManager);
        listenerContainer.setTransactionDefinition(this.transactionDefinition);
        if (this.monitorScheduler != null)
            listenerContainer.setMonitorScheduler(this.monitorScheduler);
        if (this.listenerTaskExecutor != null)
            listenerContainer.setListenerTaskExecutor(this.listenerTaskExecutor);
        return listenerContainer;
    }

    protected abstract T createListenerContainerInstance(CanalListenerEndpoint endpoint);

    private void configAbstractListenerEndpoint(CanalListenerEndpoint endpoint) {
        if (endpoint.getBatchListener() == null)
            ((AbstractCanalListenerEndpoint) endpoint).setBatchListener(this.defaultBatchListener);
        if (endpoint.getAutoStartup() == null)
            ((AbstractCanalListenerEndpoint) endpoint).setAutoStartup(this.defaultAutoStartup);
    }
}
