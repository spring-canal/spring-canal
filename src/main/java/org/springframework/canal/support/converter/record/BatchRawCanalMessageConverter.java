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
import com.google.protobuf.ByteString;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 橙子
 * @since 2020/12/12
 */
public class BatchRawCanalMessageConverter extends AbstractCanalMessageConverter implements BatchCanalMessageConverter<ByteString> {
    /**
     * {@inheritDoc}
     *
     * @return Type is {@link Message< List <com.google.protobuf.ByteString>> Message&lt;List&lt;ByteString>>}.
     */
    @Override
    public Message<List<Object>> toMessage(CanalEntries<ByteString> entries, Acknowledgment acknowledgment, CanalConnector connector, Type payloadType) {
        final ArrayList<Object> objects = new ArrayList<>(entries.getEntries());
        return MessageBuilder.withPayload((List<Object>) objects).build();
    }

    @Override
    public boolean canConvert(Class<?> entryClass, Type payloadType) {
        return ByteString.class.isAssignableFrom(entryClass);
    }
}
