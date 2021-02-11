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
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;

import org.springframework.canal.listener.ListenerUtils;
import org.springframework.canal.support.CanalEntries;
import org.springframework.canal.support.CanalMessageUtils;
import org.springframework.canal.support.converter.record.BatchCanalMessageConverter;
import org.springframework.canal.support.converter.record.BatchEntryCanalMessageConverter;
import org.springframework.canal.support.converter.record.BatchRawCanalMessageConverter;
import org.springframework.canal.support.converter.record.CanalMessageConverter;
import org.springframework.canal.support.converter.record.DelegatingBatchCanalMessageConverter;
import org.springframework.canal.transaction.CanalResourceHolder;
import org.springframework.canal.transaction.CanalResourceHolderUtils;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

import java.lang.reflect.Type;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * See also {@link CanalOperations}.
 *
 * @param <T> the converted payload type
 * @author 橙子
 * @since 2020/12/30
 */
public class CanalTemplate<T> implements CanalOperations<T> {
    private final CanalConnectorFactory connectorFactory;
    private final ThreadLocal<CanalResourceHolder> transactionalResourceHolder;
    private CanalMessageConverter messageConverter;
    private Type payloadType;
    private String subscribe;
    private boolean isStartRollback;
    private boolean allowNonTransactional;

    public CanalTemplate(CanalConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
        this.payloadType = connectorFactory.isLazyParseEntry() ? ByteString.class : CanalEntry.Entry.class;
        this.messageConverter = new DelegatingBatchCanalMessageConverter<>(
                new BatchEntryCanalMessageConverter(),
                new BatchRawCanalMessageConverter());
        this.transactionalResourceHolder = new ThreadLocal<>();
        this.isStartRollback = true;
    }

    public CanalMessageConverter getMessageConverter() {
        return this.messageConverter;
    }

    public void setMessageConverter(CanalMessageConverter messageConverter) {
        this.messageConverter = messageConverter;
    }

    public Type getPayloadType() {
        return this.payloadType;
    }

    public void setPayloadType(Type payloadType) {
        this.payloadType = payloadType;
    }

    public void setSubscribe(String subscribe) {
        this.subscribe = subscribe;
    }

    public boolean isStartRollback() {
        return this.isStartRollback;
    }

    public void setStartRollback(boolean startRollback) {
        this.isStartRollback = startRollback;
    }

    public boolean isAllowNonTransactional() {
        return this.allowNonTransactional;
    }

    public void setAllowNonTransactional(boolean allowNonTransactional) {
        this.allowNonTransactional = allowNonTransactional;
    }

    public CanalConnector getCanalConnector() {
        final CanalResourceHolder canalResourceHolder = getCanalResourceHolder();
        Assert.state(this.allowNonTransactional || (canalResourceHolder != null && canalResourceHolder.isSynchronizedWithTransaction()),
                "No transaction is in process;" +
                        "try run the template operation within the scope of a template.executeInTransaction() operation," +
                        "or start a transaction with @Transactional before invoking the template," +
                        "or run in a transaction started by a listener container when consuming a record.");
        if (canalResourceHolder != null) {
            if (!canalResourceHolder.isConnected())
                canalResourceHolder.connect(this.subscribe,
                        this.connectorFactory.getRetryTimes(),
                        this.connectorFactory.getRetryInterval(),
                        this.isStartRollback);
            return canalResourceHolder.getCanalConnector();
        }
        if (this.connectorFactory instanceof SmartCanalConnectorFactory)
            ((SmartCanalConnectorFactory) this.connectorFactory).setSubscribe(this.subscribe);
        final CanalConnector canalConnector = this.connectorFactory.getConnector();
        ListenerUtils.subscribeFilters(canalConnector,
                this.subscribe,
                this.connectorFactory.getRetryTimes(),
                this.connectorFactory.getRetryInterval(),
                this.isStartRollback);
        if (this.isStartRollback)
            canalConnector.rollback();
        return canalConnector;
    }

    /**
     * Timeout is infinity. Careful for throwing a {@link java.net.SocketTimeoutException} be wrapped in canal.
     *
     * @throws com.alibaba.otter.canal.protocol.exception.CanalClientException A {@link java.net.SocketTimeoutException} be wrapped.
     */
    @Override
    public List<T> blockingGetLeast() {
        return getLeast(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<T> getLeast() {
        return get(1);
    }

    /**
     * BatchSize is 1.
     */
    @Override
    public List<T> getLeast(long timeout, TimeUnit unit) {
        return get(1, timeout, unit);
    }

    /**
     * Timeout is infinity.
     *
     * @throws com.alibaba.otter.canal.protocol.exception.CanalClientException A {@link java.net.SocketTimeoutException} be wrapped.
     */
    @Override
    public List<T> blockingGet(int aimMemUnitCount) {
        return get(aimMemUnitCount, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public List<T> get(int aimMemUnitCount) {
        return execute(connector -> {
            final Message message;
            final CanalResourceHolder canalResourceHolder = getCanalResourceHolder();
            if (canalResourceHolder == null || !canalResourceHolder.hasTimeout())
                message = connector.getWithoutAck(aimMemUnitCount);
            else
                message = connector.getWithoutAck(aimMemUnitCount, canalResourceHolder.getTimeToLiveInMillis(), TimeUnit.MILLISECONDS);
            final List<T> result = get(message);
            if (canalResourceHolder == null)
                connector.ack(CanalMessageUtils.INVALID_MESSAGE_ID);
            return result;
        });
    }

    @Override
    public List<T> get(int aimMemUnitCount, long timeout, TimeUnit unit) {
        return execute(connector -> {
            final Message message;
            final CanalResourceHolder canalResourceHolder = getCanalResourceHolder();
            if (canalResourceHolder == null || !canalResourceHolder.hasTimeout())
                message = connector.getWithoutAck(aimMemUnitCount, timeout, unit);
            else
                message = connector.getWithoutAck(aimMemUnitCount, Math.min(unit.convert(canalResourceHolder.getTimeToLiveInMillis(), TimeUnit.MILLISECONDS), timeout), unit);
            final List<T> result = get(message);
            if (canalResourceHolder == null)
                connector.ack(CanalMessageUtils.INVALID_MESSAGE_ID);
            return result;
        });
    }

    @Override
    public <R> R execute(ConnectorCallback<R> callback) {
        Assert.notNull(callback, "'callback' must not be null");
        final CanalConnector canalConnector = getCanalConnector();
        try {
            return callback.doInCanal(canalConnector);
        } finally {
            if (getCanalResourceHolder() == null)
                canalConnector.disconnect();
        }
    }

    @Override
    public <R> R executeInTransaction(OperationsCallback<T, R> callback) {
        Assert.notNull(callback, "'callback' must not be null");
        Assert.state(this.transactionalResourceHolder.get() == null, "Nested calls to 'executeInTransaction' are not allowed");
        try {
            final CanalResourceHolder canalResourceHolder = new CanalResourceHolder(this.connectorFactory.getConnector());
            canalResourceHolder.setSynchronizedWithTransaction(true);
            this.transactionalResourceHolder.set(canalResourceHolder);
            canalResourceHolder.connect(this.subscribe,
                    this.connectorFactory.getRetryTimes(),
                    this.connectorFactory.getRetryInterval(),
                    this.isStartRollback);
            R result;
            try {
                result = callback.doInOperations(this);
            } catch (Exception e) {
                canalResourceHolder.rollback();
                throw e;
            }
            canalResourceHolder.commit();
            return result;
        } finally {
            CanalResourceHolderUtils.releaseResources(this.transactionalResourceHolder.get());
            this.transactionalResourceHolder.remove();
        }
    }

    private CanalResourceHolder getCanalResourceHolder() {
        if (this.transactionalResourceHolder.get() != null)
            return this.transactionalResourceHolder.get();
        if (TransactionSynchronizationManager.isActualTransactionActive())
            return CanalResourceHolderUtils.getTransactionalResourceHolder(this.connectorFactory);
        return null;
    }

    @SuppressWarnings("unchecked")
    private List<T> get(Message message) {
        final CanalEntries<?> entries = CanalMessageUtils.getEntries(message);
        if (this.payloadType.equals(entries.getEntryClass()))
            return (List<T>) entries.getEntries();
        return (List<T>) ((BatchCanalMessageConverter) this.messageConverter).toMessage(entries,
                null, null, this.payloadType).getPayload();
    }
}
