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

package org.springframework.canal.transaction;

import com.alibaba.otter.canal.client.CanalConnector;

import org.springframework.canal.listener.ListenerUtils;
import org.springframework.canal.support.CanalMessageUtils;
import org.springframework.transaction.support.ResourceHolderSupport;

import java.util.Objects;

/**
 * Holds resource {@link CanalConnector} for being bound to {@link ThreadLocal}
 * using {@link org.springframework.transaction.support.TransactionSynchronizationManager}.
 *
 * @author 橙子
 * @since 2020/12/28
 */
public class CanalResourceHolder extends ResourceHolderSupport {
    private final CanalConnector canalConnector;
    private boolean isConnected;
    private String lastSubscribe;
    private boolean containerRequired;

    public CanalResourceHolder(CanalConnector canalConnector) {
        this(canalConnector, false);
    }

    public CanalResourceHolder(CanalConnector canalConnector, boolean isConnected) {
        reset();
        this.canalConnector = canalConnector;
        this.isConnected = isConnected;
    }

    public CanalConnector getCanalConnector() {
        return this.canalConnector;
    }

    public boolean isConnected() {
        return this.isConnected;
    }

    public void connect(String subscribe, int retryTimes, long retryInterval, boolean autoRollback) {
        if (!this.isConnected || !Objects.equals(this.lastSubscribe, subscribe)) {
            ListenerUtils.subscribeFilters(this.canalConnector, subscribe, retryTimes, retryInterval, autoRollback);
            this.lastSubscribe = subscribe;
            this.isConnected = true;
        }
    }

    /**
     * Must support twice invoked idempotency.
     *
     * @see CanalResourceHolderUtils#releaseResources(CanalResourceHolder)
     */
    public void disconnect() {
        if (isOpen())
            return;
        if (isConnected()) {
            this.canalConnector.disconnect();
            this.isConnected = false;
        }
    }

    public void commit() {
        if (isOpen())
            return;
        if (isConnected())
            this.canalConnector.ack(CanalMessageUtils.INVALID_MESSAGE_ID);
    }

    public void rollback() {
        if (isConnected())
            this.canalConnector.rollback();
    }

    @Override
    public void clear() {
        super.clear();
        if (isOpen())
            setSynchronizedWithTransaction(true);
    }

    /**
     * Return whether there is any listener container controlling this holder.
     *
     * @return If true method {@link #disconnect()} and the receive part of method {@link #commit()} will lose its own control,
     * and the holder still is reachable in next transaction.
     */
    @Override
    public boolean isOpen() {
        return this.containerRequired;
    }

    /**
     * Set if the holder has been controlled by a listener container.
     */
    @Override
    public void requested() {
        this.containerRequired = true;
    }

    /**
     * Release the listener container's controlled.
     */
    @Override
    public void released() {
        this.containerRequired = false;
    }
}
