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
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * @param <T> the entity type
 * @author 橙子
 * @since 2020/12/12
 */
public class DelegatingEachCanalMessageConverter<T> implements EachCanalMessageConverter<T> {
    private final Collection<EachCanalMessageConverter<Object>> messageConverters = new ArrayList<>();

    public DelegatingEachCanalMessageConverter(EachCanalMessageConverter<?>... messageConverters) {
        setMessageConverters(messageConverters);
    }

    public void setMessageConverters(EachCanalMessageConverter<?>... messageConverters) {
        this.messageConverters.clear();
        this.messageConverters.addAll(Arrays.asList((EachCanalMessageConverter<Object>[]) messageConverters));
    }

    public Collection<EachCanalMessageConverter<Object>> getMessageConverters() {
        return Collections.unmodifiableCollection(this.messageConverters);
    }

    @Override
    public Message<Object> toMessage(T entry, Acknowledgment acknowledgment, CanalConnector connector, Type payloadType) {
        for (EachCanalMessageConverter<Object> messageConverter : this.messageConverters) {
            if (messageConverter instanceof AbstractCanalMessageConverter
                    && !((AbstractCanalMessageConverter) messageConverter).canConvert(entry.getClass(), payloadType))
                continue;
            return messageConverter.toMessage(entry, acknowledgment, connector, payloadType);
        }
        return MessageBuilder.withPayload((Object) entry).build();
    }
}
