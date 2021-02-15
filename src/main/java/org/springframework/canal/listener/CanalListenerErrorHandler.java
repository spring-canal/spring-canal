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

package org.springframework.canal.listener;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalException;

import org.springframework.messaging.Message;

/**
 * @author 橙子
 * @since 2020/11/14
 */
@FunctionalInterface
public interface CanalListenerErrorHandler {

    /**
     * Handle the error.
     *
     * @param message   the spring-messaging message.
     * @param exception the exception the listener threw, wrapped in a {@link CanalException}.
     * @return the return value is ignored unless the annotated method has a {@code @SendTo} annotation.
     */
    Object handleError(Message<?> message, CanalException exception);

    /**
     * Handle the error.
     *
     * @param message   the spring-messaging message.
     * @param exception the exception the listener threw, wrapped in a {@link CanalException}.
     * @param connector the consumer.
     * @return the return value is ignored unless the annotated method has a
     * {@code @SendTo} annotation.
     */
    default Object handleError(Message<?> message, CanalException exception, CanalConnector connector) {
        return handleError(message, exception);
    }
}
