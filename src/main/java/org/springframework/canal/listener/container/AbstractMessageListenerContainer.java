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

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanNameAware;
import org.springframework.canal.config.ContainerProperties;
import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.canal.listener.MessageListener;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author 橙子
 * @since 2020/11/15
 */
public abstract class AbstractMessageListenerContainer implements MessageListenerContainer, BeanNameAware, ApplicationEventPublisherAware {
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
    protected final CanalConnectorFactory connectorFactory;
    protected final ContainerProperties containerProperties;
    protected final Collection<String> subscribes;
    protected final Properties supersedeProperties;
    protected final MessageListener<?> listener;
    protected final AtomicReference<AsyncTaskExecutor> listenerTaskExecutor;
    protected volatile boolean running;
    private boolean autoStartup;
    private String beanName;
    private ApplicationEventPublisher applicationEventPublisher;
    private CanalListenerContainerErrorHandler<Object> errorHandler;
    private PlatformTransactionManager transactionManager;
    private TransactionDefinition transactionDefinition;
    private TaskScheduler monitorScheduler;

    protected AbstractMessageListenerContainer(CanalConnectorFactory connectorFactory,
                                               ContainerProperties containerProperties, MessageListener<?> listener,
                                               Collection<String> subscribes, Properties supersedeProperties) {
        this.connectorFactory = connectorFactory;
        this.containerProperties = containerProperties;
        this.listener = listener;
        this.subscribes = subscribes;
        this.supersedeProperties = supersedeProperties;
        this.listenerTaskExecutor = new AtomicReference<>();
        this.autoStartup = true;
    }

    public CanalListenerContainerErrorHandler<Object> getErrorHandler() {
        return this.errorHandler;
    }

    public void setErrorHandler(CanalListenerContainerErrorHandler<?> errorHandler) {
        this.errorHandler = (CanalListenerContainerErrorHandler<Object>) errorHandler;
    }

    public PlatformTransactionManager getTransactionManager() {
        return this.transactionManager;
    }

    public void setTransactionManager(PlatformTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public TransactionDefinition getTransactionDefinition() {
        return this.transactionDefinition;
    }

    public void setTransactionDefinition(TransactionDefinition transactionDefinition) {
        this.transactionDefinition = transactionDefinition;
    }

    public TaskScheduler getMonitorScheduler() {
        return this.monitorScheduler;
    }

    public void setMonitorScheduler(TaskScheduler monitorScheduler) {
        this.monitorScheduler = monitorScheduler;
    }

    /**
     * @return stable value after method {@link #start()} invoked.
     */
    public AsyncTaskExecutor getListenerTaskExecutor() {
        return this.listenerTaskExecutor.get();
    }

    public void setListenerTaskExecutor(AsyncTaskExecutor listenerTaskExecutor) {
        this.listenerTaskExecutor.set(listenerTaskExecutor);
    }

    public String getBeanName() {
        return this.beanName;
    }

    public ApplicationEventPublisher getApplicationEventPublisher() {
        return this.applicationEventPublisher;
    }

    @Override
    public void stop() {
        stop(true);
    }

    public abstract void stop(boolean wait);

    @Override
    public boolean isAutoStartup() {
        return this.autoStartup;
    }

    @Override
    public void setAutoStartup(boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public void setBeanName(String name) {
        this.beanName = name;
    }

    @Override
    public void setApplicationEventPublisher(ApplicationEventPublisher applicationEventPublisher) {
        this.applicationEventPublisher = applicationEventPublisher;
    }
}
