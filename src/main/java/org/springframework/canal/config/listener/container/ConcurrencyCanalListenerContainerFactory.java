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

package org.springframework.canal.config.listener.container;

import org.springframework.canal.config.endpoint.CanalListenerEndpoint;
import org.springframework.canal.listener.MessageListener;
import org.springframework.canal.listener.container.ConcurrencyMessageListenerContainer;

/**
 * A {@link CanalListenerContainerFactory} implementation to build a
 * {@link ConcurrencyMessageListenerContainer}.
 * <p>
 * This should be the default for most users and a good transition paths for those that
 * are used to building such container definitions manually.
 * <p>
 * This factory is primarily for building containers for {@link org.springframework.canal.annotation.listener.CanalListener CanalListener}
 * annotated methods but can also be used to create any container.
 * <p>
 * Only containers for {@link org.springframework.canal.annotation.listener.CanalListener CanalListener} annotated methods
 * are added to the {@link org.springframework.canal.config.endpoint.register.CanalListenerEndpointRegistry CanalListenerEndpointRegistry}.
 *
 * @param <T> the listener container's generic type
 * @author 橙子
 * @since 2020/11/17
 */
public class ConcurrencyCanalListenerContainerFactory<T> extends AbstractCanalListenerContainerFactory<ConcurrencyMessageListenerContainer<T>> {
    @Override
    public ConcurrencyMessageListenerContainer<T> createListenerContainerInstance(CanalListenerEndpoint endpoint) {
        final ConcurrencyMessageListenerContainer<T> container = new ConcurrencyMessageListenerContainer<>(this.connectorFactory,
                this.containerProperties,
                (MessageListener<?>) endpoint.createMessageListener(this.messageConverter, this.batchToEachAdapter, this.messageFilterStrategy),
                endpoint.getSubscribes(),
                endpoint.getSupersedeProperties());
        if (endpoint.getConcurrency() != null)
            container.setConcurrency(endpoint.getConcurrency());
        return container;
    }

}
