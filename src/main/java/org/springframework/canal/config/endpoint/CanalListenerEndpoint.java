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

package org.springframework.canal.config.endpoint;

import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.canal.listener.adapter.BatchToEachAdapter;
import org.springframework.canal.listener.adapter.MessageFilterStrategy;
import org.springframework.canal.support.converter.record.CanalMessageConverter;

import java.util.Collection;
import java.util.Properties;

/**
 * @author 橙子
 * @since 2020/11/14
 */
public interface CanalListenerEndpoint {
    /**
     * @return the container id of this endpoint. The id can be further qualified
     * when the endpoint is resolved against its actual listener container.
     * @see CanalListenerContainerFactory#createListenerContainer
     */
    String getId();

    /**
     * @return the container group of this endpoint or null if not in a group.
     */
    String getGroup();

    /**
     * @return the subscribes for this endpoint.
     */
    Collection<String> getSubscribes();

    /**
     * Return the concurrency for this endpoint's container.
     *
     * @return the concurrency.
     */
    Integer getConcurrency();

    /**
     * Return the autoStartup for this endpoint's container.
     *
     * @return the autoStartup.
     */
    Boolean getAutoStartup();

    /**
     * Return the batchListener for this endpoint.
     *
     * @return the batchListener.
     */
    Boolean getBatchListener();

    /**
     * Get the supersede properties that will be merged with the consumer/container properties
     * provided by the consumer/container factory; properties here will supersede any with the same
     * name(s) in the consumer/container factory.
     *
     * @return the properties.
     * @see org.springframework.canal.config.ConsumerProperties
     * @see org.springframework.canal.config.ContainerProperties
     */
    Properties getSupersedeProperties();

    Object createMessageListener(CanalMessageConverter messageConverter, BatchToEachAdapter<?> batchToEachAdapter, MessageFilterStrategy<?> filterStrategy);
}
