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

import org.apache.commons.logging.LogFactory;

import org.springframework.canal.support.CanalMessageUtils;
import org.springframework.core.log.LogAccessor;

/**
 * @param <T> the fetched record/records type
 * @author 橙子
 * @since 2020/12/1
 */
public class LoggingCanalListenerContainerEachErrorHandler<T> implements CanalListenerContainerEachErrorHandler<T> {
    private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(LoggingCanalListenerContainerEachErrorHandler.class));
    private final boolean metadataOnly;

    public LoggingCanalListenerContainerEachErrorHandler(boolean metadataOnly) {
        this.metadataOnly = metadataOnly;
    }

    @Override
    public void handle(Exception thrownException, T data) {
        LOGGER.error(thrownException, () -> "Error while processing: " + CanalMessageUtils.toString(data, this.metadataOnly));
    }
}
