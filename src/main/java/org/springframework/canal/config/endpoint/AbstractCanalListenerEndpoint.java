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

package org.springframework.canal.config.endpoint;

import org.apache.commons.logging.LogFactory;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.canal.listener.BatchMessageListener;
import org.springframework.canal.listener.CanalListenerErrorHandler;
import org.springframework.canal.listener.EachMessageListener;
import org.springframework.canal.listener.MessageListener;
import org.springframework.canal.listener.adapter.BatchMessageListenerAdapter;
import org.springframework.canal.listener.adapter.BatchToEachAdapter;
import org.springframework.canal.listener.adapter.EachMessageListenerAdapter;
import org.springframework.canal.listener.adapter.MessageFilterStrategy;
import org.springframework.canal.support.converter.record.BatchCanalMessageConverter;
import org.springframework.canal.support.converter.record.CanalMessageConverter;
import org.springframework.canal.support.converter.record.EachCanalMessageConverter;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.core.log.LogAccessor;
import org.springframework.expression.BeanResolver;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public abstract class AbstractCanalListenerEndpoint implements CanalListenerEndpoint, BeanFactoryAware, InitializingBean {
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass()));
    private MessageHandlerMethodFactory messageHandlerMethodFactory;
    private CanalListenerErrorHandler errorHandler;
    private BeanFactory beanFactory;
    private BeanExpressionResolver resolver;
    private BeanExpressionContext expressionContext;
    private BeanFactoryResolver beanResolver;
    // --below are based on @CanalListener methods--
    private String id;
    private String group;
    private final Collection<String> subscribes = new ArrayList<>();
    private Integer concurrency;
    private Boolean autoStartup;
    private Boolean batchListener;
    private Properties supersedeProperties;

    @Override
    public void afterPropertiesSet() {
        if (this.subscribes.isEmpty())
            this.logger.info("Subscribes is empty, filtered schemas follows Canal server.");
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            this.resolver = ((ConfigurableListableBeanFactory) beanFactory).getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext((ConfigurableListableBeanFactory) beanFactory, null);
        }
        this.beanResolver = new BeanFactoryResolver(beanFactory);
    }

    protected MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
        return this.messageHandlerMethodFactory;
    }

    protected BeanFactory getBeanFactory() {
        return this.beanFactory;
    }

    protected BeanExpressionResolver getResolver() {
        return this.resolver;
    }

    protected BeanExpressionContext getExpressionContext() {
        return this.expressionContext;
    }

    protected BeanResolver getBeanResolver() {
        return this.beanResolver;
    }

    /**
     * Set the {@link MessageHandlerMethodFactory} to use to build the
     * {@link InvocableHandlerMethod} responsible to manage the invocation
     * of this endpoint(handle messages {@link org.springframework.messaging.Message}).
     *
     * @param messageHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
     */
    public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory messageHandlerMethodFactory) {
        this.messageHandlerMethodFactory = messageHandlerMethodFactory;
    }

    public CanalListenerErrorHandler getErrorHandler() {
        return this.errorHandler;
    }

    /**
     * Set the {@link CanalListenerErrorHandler} to invoke if the listener method
     * throws an exception.
     *
     * @param errorHandler the error handler.
     */
    public void setErrorHandler(CanalListenerErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    // --below are based on @CanalListener methods--

    @Override
    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getGroup() {
        return this.group;
    }

    /**
     * Set the group for the corresponding listener container.
     *
     * @param group the group.
     */
    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public Collection<String> getSubscribes() {
        return Collections.unmodifiableCollection(this.subscribes);
    }

    public void setSubscribes(String... subscribes) {
        Assert.notNull(subscribes, "'subscribes' must not be null");
        this.subscribes.clear();
        this.subscribes.addAll(Arrays.asList(subscribes));
    }

    @Override
    public Integer getConcurrency() {
        return this.concurrency;
    }

    /**
     * Set the concurrency for this endpoint's container.
     *
     * @param concurrency the concurrency.
     */
    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    @Override
    public Boolean getAutoStartup() {
        return this.autoStartup;
    }

    /**
     * Set the autoStartup for this endpoint's container.
     *
     * @param autoStartup the autoStartup.
     */
    public void setAutoStartup(Boolean autoStartup) {
        this.autoStartup = autoStartup;
    }

    @Override
    public Boolean getBatchListener() {
        return this.batchListener;
    }

    public void setBatchListener(Boolean batchListener) {
        this.batchListener = batchListener;
    }

    @Override
    public Properties getSupersedeProperties() {
        return this.supersedeProperties;
    }

    /**
     * Set the supersede properties that will be merged with the consumer/container properties
     * provided by the consumer/container factory; properties here will supersede any with the same
     * name(s) in the consumer/container factory.
     *
     * @param supersedeProperties the properties.
     * @see org.springframework.canal.config.ConsumerProperties
     * @see org.springframework.canal.config.ContainerProperties
     */
    public void setSupersedeProperties(Properties supersedeProperties) {
        this.supersedeProperties = supersedeProperties;
    }

    @Override
    public MessageListener<?> createMessageListener(CanalMessageConverter messageConverter, BatchToEachAdapter<?> batchToEachAdapter, MessageFilterStrategy<?> filterStrategy) {
        throw new UnsupportedOperationException("createMessageListener(messageConverter, batchToEachAdapter, filterStrategy) is not supported by this CanalListenerEndpoint");
    }

    protected <T> BatchMessageListener<T> createBatchMessageListener(InvocableHandlerMethod defaultMethod,
                                                                     List<InvocableHandlerMethod> otherMethods,
                                                                     CanalListenerErrorHandler errorHandler,
                                                                     CanalMessageConverter messageConverter,
                                                                     BatchToEachAdapter<T> batchToEachAdapter) {
        final Object bean = defaultMethod == null ? otherMethods.get(0).getBean() : defaultMethod.getBean();
        final BatchMessageListenerAdapter<T> listenerAdapter = new BatchMessageListenerAdapter<>(bean, defaultMethod, otherMethods, errorHandler);
        if (this.beanFactory != null)
            listenerAdapter.setBeanFactory(this.beanFactory);
        if (messageConverter instanceof EachCanalMessageConverter)
            listenerAdapter.setEachMessageConverter((EachCanalMessageConverter<T>) messageConverter);
        if (messageConverter instanceof BatchCanalMessageConverter)
            listenerAdapter.setBatchMessageConverter((BatchCanalMessageConverter<T>) messageConverter);
        if (batchToEachAdapter != null)
            listenerAdapter.setBatchToEachAdapter(batchToEachAdapter);
        return listenerAdapter;
    }

    protected <T> EachMessageListener<T> createEachMessageListener(InvocableHandlerMethod defaultMethod,
                                                                   List<InvocableHandlerMethod> otherMethods,
                                                                   CanalListenerErrorHandler errorHandler,
                                                                   EachCanalMessageConverter<T> messageConverter) {
        final Object bean = defaultMethod == null ? otherMethods.get(0).getBean() : defaultMethod.getBean();
        final EachMessageListenerAdapter<T> listenerAdapter = new EachMessageListenerAdapter<>(bean, defaultMethod, otherMethods, errorHandler);
        if (this.beanFactory != null)
            listenerAdapter.setBeanFactory(this.beanFactory);
        if (messageConverter != null)
            listenerAdapter.setEachMessageConverter(messageConverter);
        return listenerAdapter;
    }
}
