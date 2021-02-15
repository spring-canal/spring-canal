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

package org.springframework.canal.support.converter.message;

import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHeaders;
import org.springframework.messaging.converter.SmartMessageConverter;

import java.lang.reflect.Type;

/**
 * @author 橙子
 * @since 2020/12/31
 */
public interface CanalSmartMessageConverter extends SmartMessageConverter {

    Object fromMessage(Message<?> message, Type targetType, RowDataColumnsType rowDataColumnsType);

    @Override
    default Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
        return fromMessage(message, conversionHint == null ? targetClass : ((MethodParameter) conversionHint).getGenericParameterType(), RowDataColumnsType.AFTER);
    }

    @Override
    default Object fromMessage(Message<?> message, Class<?> targetClass) {
        return fromMessage(message, (Type) targetClass, RowDataColumnsType.AFTER);
    }

    @Override
    default Message<?> toMessage(Object payload, MessageHeaders headers, Object conversionHint) {
        return toMessage(payload, headers);
    }

    default boolean isWholeParameterRequired() {
        return false;
    }

}
