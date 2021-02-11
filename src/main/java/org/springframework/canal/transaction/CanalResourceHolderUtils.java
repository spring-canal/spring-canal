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

import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.transaction.support.ResourceHolderSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.Assert;

/**
 * @author 橙子
 * @since 2020/12/30
 */
public final class CanalResourceHolderUtils {
    private CanalResourceHolderUtils() {
    }

    public static CanalResourceHolder getTransactionalResourceHolder(CanalConnectorFactory connectorFactory) {
        Assert.notNull(connectorFactory, "Factory must not be null");
        CanalResourceHolder resourceHolder = (CanalResourceHolder) TransactionSynchronizationManager.getResource(connectorFactory);
        if (resourceHolder == null) {
            resourceHolder = new CanalResourceHolder(connectorFactory.getConnector());
            TransactionSynchronizationManager.bindResource(connectorFactory, resourceHolder);
            resourceHolder.setSynchronizedWithTransaction(true);
            /*
             Because synchronization active flag was set to true by transaction manager
             until AFTER transaction manager's method `doBegin()`
             which is the only place to be set to true from transaction manager,
             so synchronization active flag was true means invoked by canal transaction's executor or invoked by non-canal transaction,
             register a transaction synchronization for auto release (support twice invoked idempotency when in canal transaction).
             */
            if (TransactionSynchronizationManager.isSynchronizationActive())
                TransactionSynchronizationManager.registerSynchronization(new ResourceHolderSynchronization<CanalResourceHolder, CanalConnectorFactory>(resourceHolder, connectorFactory) {
                    @Override
                    protected boolean shouldReleaseBeforeCompletion() {
                        return false;
                    }

                    @Override
                    protected void releaseResource(CanalResourceHolder resourceHolder, CanalConnectorFactory resourceKey) {
                        CanalResourceHolderUtils.releaseResources(resourceHolder);
                    }
                });
        }
        return resourceHolder;
    }

    /**
     * Must support twice invoked idempotency,
     * because may register a synchronization by canal transaction's executor in method {@link #getTransactionalResourceHolder(CanalConnectorFactory)}.
     */
    public static void releaseResources(CanalResourceHolder resourceHolder) {
        if (resourceHolder != null)
            resourceHolder.disconnect();
    }
}
