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

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;

/**
 * @param <T> the entity type
 * @author 橙子
 * @since 2020/12/13
 */
@FunctionalInterface
public interface BatchAckMessageListener<T> extends BatchMessageListener<T> {
    @Override
    default void onMessage(CanalEntries<T> data) {
        throw new UnsupportedOperationException("Container should never call this");
    }

    @Override
    void onMessage(CanalEntries<T> data, Acknowledgment acknowledgment);

}
