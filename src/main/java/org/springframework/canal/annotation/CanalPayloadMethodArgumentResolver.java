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
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver;
import org.springframework.validation.Validator;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;

/**
 * @author 橙子
 * @since 2020/11/14
 */
class CanalPayloadMethodArgumentResolver extends PayloadMethodArgumentResolver {
    private final CanalSmartMessageConverter canalMessageConverter;

    CanalPayloadMethodArgumentResolver(MessageConverter converter, Validator validator, boolean payloadAnnotationRequired) {
        super(converter, validator, !payloadAnnotationRequired);
        this.canalMessageConverter = converter instanceof CanalSmartMessageConverter ? (CanalSmartMessageConverter) converter : null;
    }

    @Override
    public boolean supportsParameter(MethodParameter parameter) {
        return parameter.hasParameterAnnotation(CanalBefore.class)
                || parameter.hasParameterAnnotation(CanalAfter.class)
                || super.supportsParameter(parameter);
    }

    @Override
    public Object resolveArgument(MethodParameter parameter, Message<?> message) throws Exception {
        try {
            return super.resolveArgument(parameter, message);
        } catch (MessageConversionException e) {
            if (isEmptyPayload(message.getPayload())
                    || this.canalMessageConverter == null || !this.canalMessageConverter.isWholeParameterRequired())
                throw e;
            Object payload;
            if (parameter.hasParameterAnnotation(CanalBefore.class))
                payload = this.canalMessageConverter.fromMessage(message, parameter.getGenericParameterType(), RowDataColumnsType.BEFORE);
            else
                payload = this.canalMessageConverter.fromMessage(message, parameter.getGenericParameterType(), RowDataColumnsType.AFTER);
            if (payload == null)
                throw e;
            return payload;
        }
    }

    @Override
    protected Class<?> resolveTargetClass(MethodParameter parameter, Message<?> message) {
        // parameter: Collection<Collection<PayloadType>> message: Message<Collection<Message<ByteString>>> or Message<Collection<ByteString>>
        if (Collection.class.isAssignableFrom(parameter.getParameterType()) && Collection.class.isAssignableFrom(message.getPayload().getClass())
                && parameter.getGenericParameterType() instanceof ParameterizedType && ((ParameterizedType) parameter.getGenericParameterType()).getActualTypeArguments().length == 1) {
            final Type argument = ((ParameterizedType) parameter.getGenericParameterType()).getActualTypeArguments()[0];
            if (!ByteString.class.equals(argument) && !Message.class.equals(argument))
                // Make payload cannot be instance of payload parameter's generic type
                return Void.TYPE;
        }
        return super.resolveTargetClass(parameter, message);
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

