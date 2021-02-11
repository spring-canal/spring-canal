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

package org.springframework.canal.listener.adapter;

import org.springframework.canal.listener.MessageListener;

/**
 * @param <T> the delegate type
 * @param <E> the entry type
 * @author 橙子
 * @since 2020/12/6
 */
public abstract class AbstractFilteringMessageListenerAdapter<T extends MessageListener<?>, E> implements DelegatingMessageListenerAdapter<T> {
    private final T delegate;
    private final MessageFilterStrategy<E> filterStrategy;

    protected AbstractFilteringMessageListenerAdapter(T delegate, MessageFilterStrategy<E> filterStrategy) {
        this.delegate = delegate;
        this.filterStrategy = filterStrategy;
    }

    protected boolean filter(E entry) {
        return this.filterStrategy.filter(entry);
    }

    @Override
    public T getDelegate() {
        return this.delegate;
    }
}