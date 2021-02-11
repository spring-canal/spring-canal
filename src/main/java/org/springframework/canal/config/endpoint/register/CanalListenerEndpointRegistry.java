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

import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.canal.config.endpoint.CanalListenerEndpoint;
import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.canal.listener.container.MessageListenerContainer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates the necessary {@link MessageListenerContainer} instances for the
 * registered {@linkplain CanalListenerEndpoint endpoints}. Also manages the
 * lifecycle of the listener containers, in particular within the lifecycle
 * of the application context.
 * <p>
 * Contrary to {@link MessageListenerContainer}s created manually, listener
 * containers managed by registry are not beans in the application context and
 * are not candidates for autowiring. Use {@link #getListenerContainers()} if
 * you need to access this registry's listener containers for management purposes.
 * If you need to access to a specific message listener container, use
 * {@link #getListenerContainer(String)} with the id of the endpoint.
 * <p>
 * See also {@link CanalListenerEndpoint}, {@link MessageListenerContainer}, {@link CanalListenerContainerFactory}.
 *
 * @author 橙子
 * @since 2020/11/14
 */
public class CanalListenerEndpointRegistry implements SmartLifecycle, ApplicationListener<ContextRefreshedEvent>, ApplicationContextAware {
    private final Map<String, MessageListenerContainer> listenerContainers = new HashMap<>();
    private volatile boolean running;
    private int lastContainerPhase = MessageListenerContainer.DEFAULT_PHASE;
    private ConfigurableApplicationContext applicationContext;
    private boolean contextRefreshed;

    /**
     * Create a message listener container for the given {@link CanalListenerEndpoint}.
     * <p>This create the necessary infrastructure to honor that endpoint
     * with regards to its configuration.
     *
     * @param endpoint the endpoint to add
     * @param factory  the listener factory to use
     * @see #registerListenerContainer(CanalListenerEndpoint, CanalListenerContainerFactory, boolean)
     */
    public void registerListenerContainer(CanalListenerEndpoint endpoint, CanalListenerContainerFactory<?> factory) {
        registerListenerContainer(endpoint, factory, false);
    }

    /**
     * Create a message listener container for the given {@link CanalListenerEndpoint}.
     * <p>This create the necessary infrastructure to honor that endpoint
     * with regards to its configuration.
     * <p>The {@code startImmediately} flag determines if the container should be
     * started immediately.
     *
     * @param endpoint         the endpoint to add.
     * @param factory          the {@link CanalListenerContainerFactory} to use.
     * @param startImmediately start the container immediately if necessary
     * @see #getListenerContainers()
     * @see #getListenerContainer(String)
     */
    @SuppressWarnings({"unchecked"})
    public void registerListenerContainer(CanalListenerEndpoint endpoint, CanalListenerContainerFactory<?> factory,
                                          boolean startImmediately) {
        Assert.notNull(endpoint, "Endpoint must not be null");
        Assert.notNull(factory, "Factory must not be null");
        String id = endpoint.getId();
        Assert.hasText(id, "Endpoint id must not be empty");
        MessageListenerContainer container;
        synchronized (this.listenerContainers) {
            Assert.state(!this.listenerContainers.containsKey(id), "Another endpoint is already registered with id '" + id + "'");
            container = createListenerContainer(endpoint, factory);
            this.listenerContainers.put(id, container);
        }
        if (StringUtils.hasText(endpoint.getGroup()) && this.applicationContext != null) {
            if (!this.applicationContext.containsBean(endpoint.getGroup()))
                this.applicationContext.getBeanFactory().registerSingleton(endpoint.getGroup(), new ArrayList<>());
            this.applicationContext.getBean(endpoint.getGroup(), List.class).add(container);
        }
        if (startImmediately)
            startIfNecessary(container);
    }

    public Map<String, MessageListenerContainer> getListenerContainers() {
        return this.listenerContainers;
    }

    public MessageListenerContainer getListenerContainer(String id) {
        return this.listenerContainers.get(id);
    }

    /**
     * Create and start a new {@link MessageListenerContainer} using the specified factory.
     *
     * @param endpoint the endpoint to create a {@link MessageListenerContainer}.
     * @param factory  the {@link CanalListenerContainerFactory} to use.
     * @return the {@link MessageListenerContainer}.
     */
    protected MessageListenerContainer createListenerContainer(CanalListenerEndpoint endpoint, CanalListenerContainerFactory<?> factory) {
        MessageListenerContainer listenerContainer = factory.createListenerContainer(endpoint);
        if (listenerContainer instanceof InitializingBean)
            try {
                ((InitializingBean) listenerContainer).afterPropertiesSet();
            } catch (Exception e) {
                throw new BeanInitializationException("Failed to initialize message listener container", e);
            }
        // Ensure that the containers created by factory are at the same phase(priority)
        if (listenerContainer.isAutoStartup() &&
                listenerContainer.getPhase() != MessageListenerContainer.DEFAULT_PHASE) { // a custom phase(priority) value
            if (this.lastContainerPhase == MessageListenerContainer.DEFAULT_PHASE) // first time
                this.lastContainerPhase = listenerContainer.getPhase();
            else if (this.lastContainerPhase != listenerContainer.getPhase()) // second time
                throw new IllegalStateException("Encountered phase mismatch between container factory definitions: " + this.lastContainerPhase + " vs " + listenerContainer.getPhase());
        }
        return listenerContainer;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (event.getApplicationContext().equals(this.applicationContext))
            this.contextRefreshed = true;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) {
        if (applicationContext instanceof ConfigurableApplicationContext)
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    @Override
    public void start() {
        for (MessageListenerContainer listenerContainer : this.listenerContainers.values())
            startIfNecessary(listenerContainer);
        this.running = true;
    }

    @Override
    public void stop() {
        this.running = false;
        for (MessageListenerContainer listenerContainer : this.listenerContainers.values())
            listenerContainer.stop();
    }

    @Override
    public void stop(Runnable callback) {
        this.running = false;
        if (this.listenerContainers.isEmpty())
            callback.run();
        else
            for (MessageListenerContainer listenerContainer : this.listenerContainers.values())
                listenerContainer.stop(callback);
    }

    @Override
    public boolean isRunning() {
        return this.running;
    }

    @Override
    public int getPhase() {
        return this.lastContainerPhase;
    }

    /**
     * Auto start the specified {@link MessageListenerContainer} if it should be started
     * on startup.
     *
     * @param listenerContainer the listener container to start.
     * @see MessageListenerContainer#isAutoStartup()
     */
    private void startIfNecessary(MessageListenerContainer listenerContainer) {
        if (this.contextRefreshed || listenerContainer.isAutoStartup())
            listenerContainer.start();
    }
}
