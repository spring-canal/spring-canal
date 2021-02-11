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

package org.springframework.canal.event;

/**
 * Field consumer always be null.
 *
 * @author 橙子
 * @since 2020/12/4
 */
public class ConsumerStartFailedEvent extends ConsumerEvent {
    private static final long serialVersionUID = 7139855032951058541L;

    public ConsumerStartFailedEvent(Object source, Object container) {
        super(null, source, container);
    }

    @Override
    public <T> T getConsumer(Class<T> type) {
        throw new UnsupportedOperationException("Consumer always be null.");
    }
}
