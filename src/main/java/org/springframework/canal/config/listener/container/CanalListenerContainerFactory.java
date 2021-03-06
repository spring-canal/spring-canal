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
import org.springframework.canal.listener.container.MessageListenerContainer;

/**
 * @param <T> the listener container type
 * @author 橙子
 * @since 2020/11/14
 */
public interface CanalListenerContainerFactory<T extends MessageListenerContainer> {
    /**
     * Create a {@link MessageListenerContainer} for the given {@link CanalListenerEndpoint}.
     * Containers created using this method are added to the listener endpoint registry.
     *
     * @param endpoint the endpoint to configure
     * @return the created container
     */
    T createListenerContainer(CanalListenerEndpoint endpoint);
}
