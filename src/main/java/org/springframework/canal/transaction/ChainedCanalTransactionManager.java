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
import org.springframework.data.transaction.ChainedTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.util.Assert;

/**
 * @author 橙子
 * @since 2021/1/3
 */
public class ChainedCanalTransactionManager extends ChainedTransactionManager implements CanalAwareTransactionManager {
    private CanalAwareTransactionManager canalAwareTransactionManager;

    public ChainedCanalTransactionManager(PlatformTransactionManager... transactionManagers) {
        super(transactionManagers);
        for (PlatformTransactionManager transactionManager : transactionManagers)
            if (transactionManager instanceof CanalAwareTransactionManager) {
                Assert.isNull(this.canalAwareTransactionManager, "Only one CanalAwareTransactionManager is allowed");
                this.canalAwareTransactionManager = (CanalAwareTransactionManager) transactionManager;
            }
        Assert.notNull(this.canalAwareTransactionManager, "Exactly one CanalAwareTransactionManager is required");
    }

    @Override
    public CanalConnectorFactory getConnectorFactory() {
        return this.canalAwareTransactionManager.getConnectorFactory();
    }
}
