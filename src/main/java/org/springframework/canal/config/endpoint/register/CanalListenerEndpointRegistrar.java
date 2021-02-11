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

package org.springframework.canal.config.endpoint.register;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.canal.annotation.CanalListenerAnnotationBeanPostProcessor;
import org.springframework.canal.config.endpoint.CanalListenerEndpoint;
import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.util.Assert;
import org.springframework.validation.Validator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * An exploation api class for users to customize register properties, but can't register.
 * <p>
 * Pass {@link CanalListenerContainerFactory} to {@link CanalListenerEndpointRegistry},
 * in order to register {@link CanalListenerEndpoint}.
 *
 * @author 橙子
 * @since 2020/11/14
 */
public class CanalListenerEndpointRegistrar implements InitializingBean, BeanFactoryAware {
    private final List<CanalListenerEndpointDescriptor> endpointDescriptors = new CopyOnWriteArrayList<>();
    private volatile boolean startImmediately;
    private BeanFactory beanFactory;
    /**
     * <h3>Actually register listener:</h3>
     * Register the {@link CanalListenerEndpoint} alongside the {@link CanalListenerContainerFactory} to use to create the underlying container.
     * And immediate startup if needed.
     *
     * @see CanalListenerEndpointRegistry#createListenerContainer(CanalListenerEndpoint, CanalListenerContainerFactory)
     */
    private CanalListenerEndpointRegistry endpointRegistry;
    private CanalListenerContainerFactory<?> containerFactory;
    private String defaultContainerFactoryBeanName;
    // --below are temporarily stored fields--
    /**
     * Temporarily stored field is passed to {@link CanalListenerAnnotationBeanPostProcessor}.
     *
     * @see #setMessageHandlerMethodFactory(MessageHandlerMethodFactory)
     * @see CanalListenerAnnotationBeanPostProcessor#afterSingletonsInstantiated()
     */
    private MessageHandlerMethodFactory messageHandlerMethodFactory;
    /**
     * If need default {@link MessageHandlerMethodFactory}, this temporarily stored field is passed to {@link CanalListenerAnnotationBeanPostProcessor.MessageHandlerMethodFactoryWrapper#createDefaultMessageHandlerMethodFactory()}.
     *
     * @see #setCustomMethodArgumentResolvers(HandlerMethodArgumentResolver...)
     * @see CanalListenerAnnotationBeanPostProcessor.MessageHandlerMethodFactoryWrapper
     */
    @SuppressWarnings("JavadocReference")
    private final List<HandlerMethodArgumentResolver> customMethodArgumentResolvers = new ArrayList<>();
    /**
     * If need default {@link MessageHandlerMethodFactory}, this temporarily stored field is passed to {@link CanalListenerAnnotationBeanPostProcessor.MessageHandlerMethodFactoryWrapper#createDefaultMessageHandlerMethodFactory()}.
     *
     * @see #setValidator(Validator)
     * @see CanalListenerAnnotationBeanPostProcessor.MessageHandlerMethodFactoryWrapper
     */
    @SuppressWarnings("JavadocReference")
    private Validator validator;

    /**
     * <h3>Actually Register all listeners:</h3>
     * Register the recorded list of {@link CanalListenerEndpointDescriptor}
     * alongside the {@link CanalListenerContainerFactory} to use to create the underlying container.
     * And trigger immediate startup.
     *
     * @see #endpointRegistry
     */
    @Override
    public void afterPropertiesSet() {
        registerAllEndpoints();
    }

    /**
     * @see #afterPropertiesSet()
     */
    private void registerAllEndpoints() {
        for (CanalListenerEndpointDescriptor descriptor : this.endpointDescriptors)
            this.endpointRegistry.registerListenerContainer(descriptor.endpoint, resolveContainerFactory(descriptor));
        this.startImmediately = true;  // Trigger immediate startup
    }

    /**
     * Register immediately a new {@link CanalListenerEndpoint} or lazy record to a new {@link CanalListenerEndpointDescriptor}
     * alongside the {@link CanalListenerContainerFactory} to use to create the underlying container.
     *
     * @param endpoint the {@link CanalListenerEndpoint} instance to register.
     * @param factory  the {@link CanalListenerContainerFactory} to use.
     *                 This may be {@code null} if the default factory has to be used for that endpoint.
     */
    public void registerEndpoint(CanalListenerEndpoint endpoint, CanalListenerContainerFactory<?> factory) {
        Assert.notNull(endpoint, "Endpoint must be not be null");
        Assert.hasText(endpoint.getId(), "Endpoint id must be set");
        // Container factory may be null, we defer the resolution right before actually creating the container
        CanalListenerEndpointDescriptor descriptor = new CanalListenerEndpointDescriptor(endpoint, factory);
        if (this.startImmediately) // Register and start immediately
            this.endpointRegistry.registerListenerContainer(descriptor.endpoint, resolveContainerFactory(descriptor), true);
        else
            this.endpointDescriptors.add(descriptor);
    }

    private CanalListenerContainerFactory<?> resolveContainerFactory(CanalListenerEndpointDescriptor descriptor) {
        if (descriptor.containerFactory != null)
            return descriptor.containerFactory;
        else if (this.containerFactory != null)
            return this.containerFactory;
        else if (this.defaultContainerFactoryBeanName != null) {
            Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
            this.containerFactory = this.beanFactory.getBean(this.defaultContainerFactoryBeanName, CanalListenerContainerFactory.class);
            return this.containerFactory;  // Consider changing this if live change of the factory is required
        }
        throw new IllegalStateException("Could not resolve the " +
                CanalListenerContainerFactory.class.getSimpleName() + " to use for [" +
                descriptor.endpoint + "] no factory was given and no default is set.");
    }

    /**
     * @return the {@link CanalListenerEndpointRegistry} instance for this registrar, may be {@code null}.
     */
    public CanalListenerEndpointRegistry getEndpointRegistry() {
        return this.endpointRegistry;
    }

    /**
     * Set the {@link CanalListenerEndpointRegistry} instance to use.
     *
     * @param endpointRegistry the {@link CanalListenerEndpointRegistry} instance to use.
     */
    public void setEndpointRegistry(CanalListenerEndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
    }

    /**
     * Set the {@link CanalListenerContainerFactory} to use in case a {@link CanalListenerEndpoint}
     * is registered with a {@code null} container factory.
     * <p>Alternatively, the bean name of the {@link CanalListenerContainerFactory} to use
     * can be specified for a lazy lookup, see {@link #setDefaultContainerFactoryBeanName}.
     *
     * @param containerFactory the {@link CanalListenerContainerFactory} instance.
     */
    public void setContainerFactory(CanalListenerContainerFactory<?> containerFactory) {
        this.containerFactory = containerFactory;
    }

    // --below are temporarily stored fields--

    public String getDefaultContainerFactoryBeanName() {
        return this.defaultContainerFactoryBeanName;
    }

    /**
     * Set the bean name of the {@link CanalListenerContainerFactory} to use in case
     * a {@link CanalListenerEndpoint} is registered with a {@code null} container factory.
     * Alternatively, the container factory instance can be registered directly:
     * see {@link #setContainerFactory(CanalListenerContainerFactory)}.
     *
     * @param defaultContainerFactoryBeanName the {@link CanalListenerContainerFactory} bean name.
     * @see #setBeanFactory
     */
    public void setDefaultContainerFactoryBeanName(String defaultContainerFactoryBeanName) {
        this.defaultContainerFactoryBeanName = defaultContainerFactoryBeanName;
    }

    /**
     * Invoked by {@link CanalListenerAnnotationBeanPostProcessor#afterSingletonsInstantiated()} by default.
     * Used in {@link #resolveContainerFactory(CanalListenerEndpointDescriptor)}.
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    /**
     * @return the list of {@link HandlerMethodArgumentResolver}.
     */
    public List<HandlerMethodArgumentResolver> getCustomMethodArgumentResolvers() {
        return Collections.unmodifiableList(this.customMethodArgumentResolvers);
    }

    /**
     * Add custom methods arguments resolvers to {@link CanalListenerAnnotationBeanPostProcessor}
     * Default empty list.
     *
     * @param methodArgumentResolvers the methodArgumentResolvers to assign.
     */
    public void setCustomMethodArgumentResolvers(HandlerMethodArgumentResolver... methodArgumentResolvers) {
        this.customMethodArgumentResolvers.clear();
        this.customMethodArgumentResolvers.addAll(Arrays.asList(methodArgumentResolvers));
    }

    /**
     * @return the custom {@link MessageHandlerMethodFactory} to use, if any.
     */
    public MessageHandlerMethodFactory getMessageHandlerMethodFactory() {
        return this.messageHandlerMethodFactory;
    }

    /**
     * Set the {@link MessageHandlerMethodFactory} to use to configure the message
     * listener responsible to serve an endpoint detected by this processor.
     * <p>By default,
     * {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory}
     * is used and it can be configured further to support additional method arguments
     * or to customize conversion and validation support. See
     * {@link org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory}
     * javadoc for more details.
     *
     * @param canalHandlerMethodFactory the {@link MessageHandlerMethodFactory} instance.
     * @see CanalListenerAnnotationBeanPostProcessor.MessageHandlerMethodFactoryWrapper#createDefaultMessageHandlerMethodFactory()
     */
    @SuppressWarnings("JavadocReference")
    public void setMessageHandlerMethodFactory(MessageHandlerMethodFactory canalHandlerMethodFactory) {
        Assert.isNull(this.validator, "A validator cannot be provided with a custom message handler factory");
        this.messageHandlerMethodFactory = canalHandlerMethodFactory;
    }

    public Validator getValidator() {
        return this.validator;
    }

    /**
     * Set the validator to use if the default message handler factory is used.
     *
     * @param validator the validator.
     */
    public void setValidator(Validator validator) {
        Assert.isNull(this.messageHandlerMethodFactory, "A validator cannot be provided with a custom message handler factory");
        this.validator = validator;
    }

    /**
     * Temporarily stored field is used to {@link #registerAllEndpoints()}.
     *
     * @see #registerEndpoint(CanalListenerEndpoint, CanalListenerContainerFactory)
     */
    private static class CanalListenerEndpointDescriptor {
        private final CanalListenerEndpoint endpoint;
        private final CanalListenerContainerFactory<?> containerFactory;

        CanalListenerEndpointDescriptor(CanalListenerEndpoint endpoint, CanalListenerContainerFactory<?> containerFactory) {
            this.endpoint = endpoint;
            this.containerFactory = containerFactory;
        }
    }
}
