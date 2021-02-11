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

package org.springframework.canal.listener.adapter;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalException;

import org.springframework.canal.listener.CanalListenerErrorHandler;
import org.springframework.canal.listener.EachAckConnectorAwareMessageListener;
import org.springframework.canal.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.util.List;

/**
 * @param <T> the message converter's generic type
 * @author 橙子
 * @since 2020/12/8
 */
public class EachMessageListenerAdapter<T> extends AbstractMessageListenerAdapter<T> implements EachAckConnectorAwareMessageListener<T> {

    public EachMessageListenerAdapter(Object bean, InvocableHandlerMethod listenerMethod) {
        this(bean, listenerMethod, null);
    }

    public EachMessageListenerAdapter(Object bean, InvocableHandlerMethod listenerMethod, CanalListenerErrorHandler errorHandler) {
        super(bean, listenerMethod, errorHandler);
    }

    public EachMessageListenerAdapter(Object bean, InvocableHandlerMethod defaultHandlerMethod, List<InvocableHandlerMethod> otherHandlerMethods, CanalListenerErrorHandler errorHandler) {
        super(bean, defaultHandlerMethod, otherHandlerMethods, errorHandler);
    }

    protected void invoke(Message<?> message, Object record, Acknowledgment acknowledgment, CanalConnector connector) {
        try {
            super.invokeHandlerMethod(findHandlerMethod(record), message, record, acknowledgment, connector);
        } catch (CanalException e) {
            handleException(message, record, connector, e);
        }
    }

    @Override
    public void onMessage(T data, Acknowledgment acknowledgment, CanalConnector connector) {
        final InvocableHandlerMethod handlerMethod = findHandlerMethod(data);
        if (isConversionRequired(handlerMethod, data.getClass()))
            invoke(this.eachMessageConverter.toMessage(data, acknowledgment, connector, getParameterPayloadType(handlerMethod, data.getClass())),
                    data, acknowledgment, connector);
        else
            invoke(NULL_MESSAGE, data, acknowledgment, connector);
    }
}
