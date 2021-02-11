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

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Each method's parameter {@code 'payloadType'} should be equals to the type of generic type {@code T}.
 *
 * @param <T> the converted payload type
 * @author 橙子
 * @since 2020/12/30
 */
public interface CanalOperations<T> {
    /**
     * @see #getLeast(long, TimeUnit)
     */
    List<T> blockingGetLeast();

    /**
     * @see #getLeast(long, TimeUnit)
     */
    List<T> getLeast();

    /**
     * Get least canal binlog memory unit.
     */
    List<T> getLeast(long timeout, TimeUnit unit);

    /**
     * Timeout is infinity.
     *
     * @see #get(int, long, TimeUnit)
     */
    List<T> blockingGet(int aimMemUnitCount);

    /**
     * @see #get(int, long, TimeUnit)
     */
    List<T> get(int aimMemUnitCount);

    /**
     * @param aimMemUnitCount batchSize
     */
    List<T> get(int aimMemUnitCount, long timeout, TimeUnit unit);

    <R> R execute(ConnectorCallback<R> callback);

    /**
     * Execute callback in a new transaction which not bind to {@link org.springframework.transaction.support.TransactionSynchronizationManager}.
     */
    <R> R executeInTransaction(OperationsCallback<T, R> callback);

    interface ConnectorCallback<T> {
        T doInCanal(CanalConnector connector);
    }

    interface OperationsCallback<T, R> {
        R doInOperations(CanalOperations<T> operations);
    }
}
