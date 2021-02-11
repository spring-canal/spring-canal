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

package org.springframework.canal.support.converter.record;

import com.alibaba.otter.canal.client.CanalConnector;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;
import org.springframework.messaging.Message;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Convert {@link CanalEntries<T> CanalEntries&lt;T>} to {@link org.springframework.messaging.Message<java.util.List> Message&lt;List&lt;PayloadType>>}.
 *
 * @param <T> the entity type; can be {@link com.alibaba.otter.canal.protocol.CanalEntry CanalEntry.Entry} or {@link com.google.protobuf.ByteString ByteString}
 * @author 橙子
 * @since 2020/12/12
 */
public interface BatchCanalMessageConverter<T> extends CanalMessageConverter {
    Message<List<Object>> toMessage(CanalEntries<T> entries, Acknowledgment acknowledgment, CanalConnector connector, Type payloadType);
}
