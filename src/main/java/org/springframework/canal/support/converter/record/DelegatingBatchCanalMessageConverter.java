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
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @param <T> the entity type
 * @author 橙子
 * @since 2020/12/12
 */
public class DelegatingBatchCanalMessageConverter<T> implements BatchCanalMessageConverter<T> {
    private final Collection<BatchCanalMessageConverter<Object>> messageConverters = new ArrayList<>();

    public DelegatingBatchCanalMessageConverter(BatchCanalMessageConverter<?>... messageConverters) {
        setMessageConverters(messageConverters);
    }

    public void setMessageConverters(BatchCanalMessageConverter<?>... messageConverters) {
        this.messageConverters.clear();
        this.messageConverters.addAll(Arrays.asList((BatchCanalMessageConverter<Object>[]) messageConverters));
    }

    public Collection<BatchCanalMessageConverter<Object>> getMessageConverters() {
        return Collections.unmodifiableCollection(this.messageConverters);
    }

    @Override
    public Message<List<Object>> toMessage(CanalEntries<T> entries, Acknowledgment acknowledgment, CanalConnector connector, Type payloadType) {
        for (BatchCanalMessageConverter<Object> messageConverter : this.messageConverters) {
            if (messageConverter instanceof AbstractCanalMessageConverter
                    && !((AbstractCanalMessageConverter) messageConverter).canConvert(entries.getEntryClass(), payloadType))
                continue;
            return messageConverter.toMessage((CanalEntries<Object>) entries, acknowledgment, connector, payloadType);
        }
        return MessageBuilder.withPayload((List<Object>) entries.getEntries()).build();
    }
}
