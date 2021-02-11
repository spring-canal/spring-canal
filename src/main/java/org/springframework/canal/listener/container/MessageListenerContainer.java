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

package org.springframework.canal.listener.container;

import org.springframework.context.SmartLifecycle;

/**
 * Start a thread to consume messages using consumer {@link com.alibaba.otter.canal.client.CanalConnector}.
 *
 * @author 橙子
 * @since 2020/11/14
 */
public interface MessageListenerContainer extends SmartLifecycle {
    /**
     * The default {@link org.springframework.context.SmartLifecycle} phase(priority) for listener
     * containers {@value #DEFAULT_PHASE}.
     */
    int DEFAULT_PHASE = Integer.MAX_VALUE - 100; // late phase(priority)

    @Override
    default int getPhase() {
        return MessageListenerContainer.DEFAULT_PHASE;
    }

    default void setAutoStartup(boolean autoStartup) {
    }
}
