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

package org.springframework.canal.annotation;

import com.google.protobuf.ByteString;

import org.springframework.canal.support.converter.message.CanalSmartMessageConverter;
import org.springframework.canal.support.converter.message.RowDataColumnsType;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

/**
 * @author 橙子
 * @since 2020/12/31
 */
class CanalMessageMethodArgumentResolver extends MessageMethodArgumentResolver {
    private final ThreadLocal<Type> targetType;
    private final CanalSmartMessageConverter canalMessageConverter;

    CanalMessageMethodArgumentResolver(MessageConverter converter) {
        super(converter);
        this.targetType = new ThreadLocal<>();
        this.canalMessageConverter = converter instanceof CanalSmartMessageConverter ? (CanalSmartMessageConverter) converter : null;
    }

    @Override
    public Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
        try {
            return super.resolveArgument(parameter, message);
        } catch (MessageConversionException e) {
            if (isEmptyPayload(message.getPayload())
                    || this.canalMessageConverter == null || !this.canalMessageConverter.isWholeParameterRequired())
                throw e;
            final Object payload = this.canalMessageConverter.fromMessage(message, this.targetType.get(), RowDataColumnsType.AFTER);
            if (payload == null)
                throw e;
            return MessageBuilder.createMessage(payload, message.getHeaders());
        } finally {
            this.targetType.remove();
        }
    }

    @Override
    public Class<?> getPayloadType(MethodParameter parameter, Message<?> message) {
        // parameter: Message<Collection<Collection<PayloadType>>> message: Message<Collection<Message<ByteString>>> or Message<Collection<ByteString>>
        ResolvableType resolvableType = ResolvableType.forType(parameter.getGenericParameterType()).as(Message.class).getGeneric();
        Class<?> cls = resolvableType.toClass();
        Type type = resolvableType.getType();
        this.targetType.set(type);
        if (Collection.class.isAssignableFrom(cls) && Collection.class.isAssignableFrom(message.getPayload().getClass())
                && type instanceof ParameterizedType && ((ParameterizedType) type).getActualTypeArguments().length == 1) {
            Type argument = ((ParameterizedType) type).getActualTypeArguments()[0];
            if (!ByteString.class.equals(argument) && !Message.class.equals(argument))
                // Make message's payload cannot be instance of message parameter's generic type
                return Void.TYPE;
        }
        return cls;
    }

    @Override
    protected boolean isEmptyPayload(Object payload) {
        if (super.isEmptyPayload(payload))
            return true;
        if (payload instanceof ByteString)
            return ((ByteString) payload).isEmpty();
        return false;
    }
}
