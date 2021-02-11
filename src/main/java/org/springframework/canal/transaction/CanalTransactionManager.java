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

import com.alibaba.otter.canal.protocol.exception.CanalClientException;

import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.InvalidIsolationLevelException;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;
import org.springframework.transaction.support.SmartTransactionObject;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionSynchronizationUtils;

/**
 * @author 橙子
 * @since 2020/12/28
 */
public class CanalTransactionManager extends AbstractPlatformTransactionManager implements CanalAwareTransactionManager {
    private final transient CanalConnectorFactory connectorFactory;

    public CanalTransactionManager(CanalConnectorFactory connectorFactory) {
        this.connectorFactory = connectorFactory;
    }

    @Override
    public CanalConnectorFactory getConnectorFactory() {
        return this.connectorFactory;
    }

    @Override
    protected Object doGetTransaction() {
        final CanalTransactionObject txObject = new CanalTransactionObject();
        txObject.setResourceHolder((CanalResourceHolder) TransactionSynchronizationManager.getResource(this.connectorFactory));
        return txObject;
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        return ((CanalTransactionObject) transaction).canalResourceHolder != null
                && ((CanalTransactionObject) transaction).canalResourceHolder.isSynchronizedWithTransaction();
    }

    /**
     * {@inheritDoc}
     *
     * <p>When {@link #isExistingTransaction(Object)} return true(means in sub-transaction):
     * <ul>
     * <li>if propagation behavior is {@link TransactionDefinition#PROPAGATION_REQUIRES_NEW REQUIRES_NEW},
     * new transaction will invoking this method to suspend old transaction
     * before method {@link #doBegin(Object, TransactionDefinition)} invoked.</li>
     * <li>if propagation behavior is {@link TransactionDefinition#PROPAGATION_NOT_SUPPORTED NOT_SUPPORTED},
     * sub-transaction (non-transaction) will invoking this method to suspend parent-transaction too.</li>
     * </ul>
     *
     * @see AbstractPlatformTransactionManager#handleExistingTransaction(TransactionDefinition, Object, boolean)
     */
    @SuppressWarnings("JavadocReference")
    @Override
    protected Object doSuspend(Object transaction) {
        ((CanalTransactionObject) transaction).setResourceHolder(null);
        return TransactionSynchronizationManager.unbindResource(this.connectorFactory);
    }

    /**
     * {@inheritDoc}
     *
     * <p>Sub-transaction or new transaction will invoking this method to resume parent-transaction or old transaction in finally block
     * for method {@link #doSuspend(Object)} work when next sub-transaction or new transaction.
     */
    @Override
    protected void doResume(Object transaction, Object suspendedResources) {
        TransactionSynchronizationManager.bindResource(this.connectorFactory, suspendedResources);
    }

    /**
     * @param transaction {@inheritDoc}
     *
     *                    <p>Maybe was suspended by method {@link #doSuspend(Object)}.
     */
    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        if (definition.getIsolationLevel() != TransactionDefinition.ISOLATION_DEFAULT)
            throw new InvalidIsolationLevelException("Canal does not support an isolation level concept");
        final CanalTransactionObject txObject = (CanalTransactionObject) transaction;
        CanalResourceHolder currentOrNewHolder = null;
        try {
            currentOrNewHolder = CanalResourceHolderUtils.getTransactionalResourceHolder(this.connectorFactory);
            final int timeout = determineTimeout(definition);
            if (TransactionDefinition.TIMEOUT_DEFAULT != timeout)
                currentOrNewHolder.setTimeoutInSeconds(timeout);
            txObject.setResourceHolder(currentOrNewHolder);
        } catch (Exception e) {
            if (currentOrNewHolder != null)
                CanalResourceHolderUtils.releaseResources(currentOrNewHolder);
            throw new CannotCreateTransactionException("Could not create Canal transaction", e);
        }
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        ((CanalTransactionObject) status.getTransaction()).canalResourceHolder.commit();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        try {
            ((CanalTransactionObject) status.getTransaction()).canalResourceHolder.rollback();
        } catch (CanalClientException e) {
            throw new TransactionSystemException("Could not roll back Canal transaction", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>When sub-transaction need rollback, it will invoking this method to mark parent-transaction,
     * to invoking method {@link #processRollback(DefaultTransactionStatus, boolean)} instead of {@link #processCommit(DefaultTransactionStatus)}
     * when parent-transaction commit.
     *
     * @see #commit(TransactionStatus)
     * @see DefaultTransactionStatus#isGlobalRollbackOnly()
     */
    @SuppressWarnings("JavadocReference")
    @Override
    protected void doSetRollbackOnly(DefaultTransactionStatus status) {
        ((CanalTransactionObject) status.getTransaction()).canalResourceHolder.setRollbackOnly();
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        final CanalTransactionObject txObject = (CanalTransactionObject) transaction;
        if (!txObject.canalResourceHolder.isOpen()) {
            TransactionSynchronizationManager.unbindResource(this.connectorFactory);
            txObject.canalResourceHolder.clear();
        }
        CanalResourceHolderUtils.releaseResources(txObject.canalResourceHolder);
    }

    /**
     * Canal transaction object, representing a CanalResourceHolder. Used as transaction object by
     * CanalTransactionManager.
     *
     * @see CanalResourceHolder
     */
    private static class CanalTransactionObject implements SmartTransactionObject {
        private CanalResourceHolder canalResourceHolder;

        public void setResourceHolder(CanalResourceHolder canalResourceHolder) {
            this.canalResourceHolder = canalResourceHolder;
        }

        @Override
        public boolean isRollbackOnly() {
            return this.canalResourceHolder.isRollbackOnly();
        }

        @Override
        public void flush() {
            if (TransactionSynchronizationManager.isSynchronizationActive())
                TransactionSynchronizationUtils.triggerFlush();
        }
    }
}
