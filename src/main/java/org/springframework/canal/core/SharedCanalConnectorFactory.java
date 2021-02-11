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

package org.springframework.canal.core;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;

import org.springframework.beans.factory.DisposableBean;
import org.springframework.canal.config.ConsumerProperties;
import org.springframework.canal.listener.ListenerUtils;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author 橙子
 * @since 2021/2/1
 */
public class SharedCanalConnectorFactory extends SimpleCanalConnectorFactory implements SmartCanalConnectorFactory, DisposableBean {
    private final Map<String, SubscribedCanalConnector> cache = new ConcurrentHashMap<>();
    private final AtomicReference<SubscribedCanalConnector> nullCache = new AtomicReference<>();
    private final ThreadLocal<OptionalSubscribe> subscribe = new ThreadLocal<>();
    private boolean isStartRollback;

    public SharedCanalConnectorFactory(ConsumerProperties properties) {
        super(properties);
        this.isStartRollback = true;
    }

    @Override
    public void setSubscribe(String subscribe) {
        this.subscribe.set(OptionalSubscribe.of(subscribe));
    }

    @Override
    public void setStartRollback(boolean startRollback) {
        this.isStartRollback = startRollback;
    }

    @Override
    public CanalConnector getObject() {
        final OptionalSubscribe optionalSubscribe = this.subscribe.get();
        if (optionalSubscribe == null)
            return super.getObject();
        if (optionalSubscribe == OptionalSubscribe.EMPTY)
            return getSingletonNull();
        return this.cache.computeIfAbsent(optionalSubscribe.getValue(), v -> new SubscribedCanalConnector(super.getObject(), v));
    }

    @Override
    public void destroy() {
        this.cache.values().forEach(SubscribedCanalConnector::destroy);
        final SubscribedCanalConnector connector = this.nullCache.getAndSet(null);
        if (connector != null)
            connector.destroy();
        this.cache.clear();
        this.subscribe.remove();
    }

    private SubscribedCanalConnector getSingletonNull() {
        SubscribedCanalConnector connector = this.nullCache.get();
        if (connector == null)
            synchronized (this.nullCache) {
                connector = this.nullCache.get();
                if (connector == null) {
                    connector = new SubscribedCanalConnector(super.getObject(), null);
                    this.nullCache.set(connector);
                }
            }
        return connector;
    }

    protected class SubscribedCanalConnector implements CanalConnector, DisposableBean {
        private final CanalConnector delegate;
        private final String subscribe;

        protected SubscribedCanalConnector(CanalConnector delegate, String subscribe) {
            this.delegate = delegate;
            this.subscribe = subscribe;
            ListenerUtils.subscribeFilters(this.delegate, this.subscribe, SharedCanalConnectorFactory.this.getRetryTimes(),
                    SharedCanalConnectorFactory.this.getRetryInterval(), SharedCanalConnectorFactory.this.isStartRollback);
        }

        @Override
        public void connect() {
            this.delegate.connect();
        }

        @Override
        public void disconnect() {
            // Do nothing because of subscribe shared.
        }

        @Override
        public void destroy() {
            this.delegate.disconnect();
        }

        @Override
        public boolean checkValid() {
            return this.delegate.checkValid();
        }

        @Override
        public void subscribe(String filter) {
            if (this.subscribe.equals(filter))
                return;
            throw new UnsupportedOperationException("Already subscribed");
        }

        @Override
        public void subscribe() {
            if (this.subscribe == null)
                return;
            throw new UnsupportedOperationException("Already subscribed");
        }

        @Override
        public void unsubscribe() {
            // Do nothing because of subscribe shared.
        }

        @Override
        public Message get(int batchSize) {
            return this.delegate.get(batchSize);
        }

        @Override
        public Message get(int batchSize, Long timeout, TimeUnit unit) {
            return this.delegate.get(batchSize, timeout, unit);
        }

        @Override
        public Message getWithoutAck(int batchSize) {
            return this.delegate.getWithoutAck(batchSize);
        }

        @Override
        public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) {
            return this.delegate.getWithoutAck(batchSize, timeout, unit);
        }

        @Override
        public void ack(long batchId) {
            this.delegate.ack(batchId);
        }

        @Override
        public void rollback(long batchId) {
            this.delegate.rollback(batchId);
        }

        @Override
        public void rollback() {
            this.delegate.rollback();
        }
    }

    private static final class OptionalSubscribe {
        public static final OptionalSubscribe EMPTY = new OptionalSubscribe(null);
        private final String value;

        private OptionalSubscribe(String value) {
            this.value = value;
        }

        public String getValue() {
            return this.value;
        }

        public static OptionalSubscribe of(String subscribe) {
            return subscribe == null ? OptionalSubscribe.EMPTY : new OptionalSubscribe(subscribe);
        }
    }
}
