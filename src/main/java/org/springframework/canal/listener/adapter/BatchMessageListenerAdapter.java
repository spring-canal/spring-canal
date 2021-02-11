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
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;

import org.springframework.canal.listener.BatchAckConnectorAwareMessageListener;
import org.springframework.canal.listener.CanalListenerErrorHandler;
import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;
import org.springframework.canal.support.converter.record.BatchCanalMessageConverter;
import org.springframework.canal.support.converter.record.BatchEntryCanalMessageConverter;
import org.springframework.canal.support.converter.record.BatchRawCanalMessageConverter;
import org.springframework.canal.support.converter.record.DelegatingBatchCanalMessageConverter;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.Assert;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * @param <T> the message converter's generic type
 * @author 橙子
 * @since 2020/12/8
 */
public class BatchMessageListenerAdapter<T> extends AbstractMessageListenerAdapter<T> implements BatchAckConnectorAwareMessageListener<T> {
    @SuppressWarnings("rawtypes")
    protected static final Type MESSAGE_COLLECTION_TYPE = ResolvableType.forType(new ParameterizedTypeReference<Collection<Message>>() {
    }).getType();
    protected static final Type ENTRY_COLLECTION_TYPE = ResolvableType.forType(new ParameterizedTypeReference<Collection<CanalEntry.Entry>>() {
    }).getType();
    protected static final Type RAW_COLLECTION_TYPE = ResolvableType.forType(new ParameterizedTypeReference<Collection<ByteString>>() {
    }).getType();
    private BatchCanalMessageConverter<T> batchMessageConverter = new DelegatingBatchCanalMessageConverter<>(new BatchEntryCanalMessageConverter(), new BatchRawCanalMessageConverter());
    private BatchToEachAdapter<T> batchToEachAdapter;
    private boolean rawEntry;

    public BatchMessageListenerAdapter(Object bean, InvocableHandlerMethod defaultHandlerMethod) {
        this(bean, defaultHandlerMethod, null);
    }

    public BatchMessageListenerAdapter(Object bean, InvocableHandlerMethod defaultHandlerMethod, CanalListenerErrorHandler errorHandler) {
        super(bean, defaultHandlerMethod, errorHandler);
    }

    public BatchMessageListenerAdapter(Object bean, InvocableHandlerMethod defaultHandlerMethod, List<InvocableHandlerMethod> otherHandlerMethods, CanalListenerErrorHandler errorHandler) {
        super(bean, defaultHandlerMethod, otherHandlerMethods, errorHandler);
    }

    public void setBatchMessageConverter(BatchCanalMessageConverter<T> batchMessageConverter) {
        this.batchMessageConverter = batchMessageConverter;
    }

    public void setBatchToEachAdapter(BatchToEachAdapter<T> batchToEachAdapter) {
        this.batchToEachAdapter = batchToEachAdapter;
    }

    public boolean isEntriesRequired(InvocableHandlerMethod handlerMethod) {
        return super.getParameterSimilarTypeCount(handlerMethod, this.rawEntry ? RAW_COLLECTION_TYPE : ENTRY_COLLECTION_TYPE, CanalEntries.class) != 0;
    }

    /**
     * @return If any parameter type contains {@link List<Message> List&lt;Message>},
     * {@link List<Message> List&lt;Message&lt;?>>}, {@link List<Message> List&lt;Message&lt;T>>},
     * {@link List<Message> List&lt;? extends Message>}, {@link List<Message> List&lt;? extends Message&lt;?>>},
     * {@link List<Message> List&lt;? extends Message&lt;T>>},
     * {@link List<Message> List&lt;T extends Message>}, {@link List<Message> List&lt;T extends Message&lt;?>>}
     * or {@link List<Message> List&lt;E extends Message&lt;T>>}.
     */
    public boolean isMessagesRequired(InvocableHandlerMethod handlerMethod) {
        return super.getParameterSimilarTypeCount(handlerMethod, MESSAGE_COLLECTION_TYPE) != 0;
    }

    public boolean isOriginTypeRequired(InvocableHandlerMethod handlerMethod) {
        return super.isSimilarType(getParameterPayloadType(handlerMethod), com.alibaba.otter.canal.protocol.Message.class);
    }

    protected void invoke(Message<?> message, Object recordsOrRecord, Acknowledgment acknowledgment, CanalConnector connector) {
        try {
            super.invokeHandlerMethod(findHandlerMethod(recordsOrRecord), message, recordsOrRecord, acknowledgment, connector);
        } catch (CanalException e) {
            handleException(message, recordsOrRecord, connector, e);
        }
    }

    @Override
    public boolean wantsOriginMessage(Object originData) {
        return isOriginTypeRequired(findHandlerMethod(originData));
    }

    @Override
    public void setRawEntry(boolean rawEntry) {
        this.rawEntry = rawEntry;
    }

    @Override
    public void onMessage(com.alibaba.otter.canal.protocol.Message records, Acknowledgment acknowledgment, CanalConnector connector) {
        invoke(NULL_MESSAGE, records, acknowledgment, connector);
    }

    @Override
    public void onMessage(CanalEntries<T> data, Acknowledgment acknowledgment, CanalConnector connector) {
        final InvocableHandlerMethod handlerMethod = findHandlerMethod(data);
        if (isConversionRequired(handlerMethod)) {
            Message<?> message;
            if (isMessagesRequired(handlerMethod)) {
                final List<Message<?>> messages = new ArrayList<>(data.size());
                for (T entry : data)
                    messages.add(this.eachMessageConverter.toMessage(entry, acknowledgment, connector, getParameterPayloadType(handlerMethod)));
                if (this.batchToEachAdapter != null) {
                    this.batchToEachAdapter.adapt(messages, data, acknowledgment, connector, this::invoke);
                    return;
                }
                message = MessageBuilder.withPayload(messages).build();
            } else
                message = this.batchMessageConverter.toMessage(data, acknowledgment, connector, getParameterPayloadType(handlerMethod));
            logger.debug(() -> "Processing [" + message + "]");
            invoke(message, data, acknowledgment, connector);
        } else
            invoke(MessageBuilder.withPayload(data.getEntries()).build(), data, acknowledgment, connector);
    }

    @Override
    public boolean isConversionRequired(InvocableHandlerMethod handlerMethod, Type... exceptedTypes) {
        final int ignoreTypeCount = getParameterSimilarTypeCount(handlerMethod, super.appendExceptedTypes());
        final int originTypeCount = getParameterSimilarTypeCount(handlerMethod, this.rawEntry ? RAW_COLLECTION_TYPE : ENTRY_COLLECTION_TYPE, CanalEntries.class);
        if (originTypeCount > 1 || ignoreTypeCount + 1 > handlerMethod.getMethodParameters().length) {
            String stateMessage = "A parameter of type '%s' must be the only parameter "
                    + "(except for an optional 'Acknowledgment' and/or 'CanalConnector')";
            Assert.state(!isOriginTypeRequired(handlerMethod),
                    () -> String.format(stateMessage, com.alibaba.otter.canal.protocol.Message.class.getName()));
            Assert.state(!isEntriesRequired(handlerMethod),
                    () -> String.format(stateMessage, (this.rawEntry ? RAW_COLLECTION_TYPE : ENTRY_COLLECTION_TYPE).getTypeName() + " or " + CanalEntries.class.getName()));
            Assert.state(!isMessagesRequired(handlerMethod),
                    () -> String.format(stateMessage, MESSAGE_COLLECTION_TYPE.getTypeName()));
        }
        return ignoreTypeCount + originTypeCount != handlerMethod.getMethodParameters().length;
    }

    @Override
    protected final Object doInvokeHandlerMethod(InvocableHandlerMethod handlerMethod, Message<?> message, Object originData, Acknowledgment acknowledgment, CanalConnector connector) throws Exception {
        if ((originData instanceof Collection || originData instanceof CanalEntries) && !isEntriesRequired(handlerMethod)) {
            if (handlerMethod.equals(this.defaultHandlerMethod))
                // Needed to avoid returning raw Message which matches Object
                return handlerMethod.invoke(message, message.getPayload(), acknowledgment, connector);
            return handlerMethod.invoke(message, acknowledgment, connector);
        }
        if (isMetadataRequired(handlerMethod)) {
            if (handlerMethod.equals(this.defaultHandlerMethod))
                // Needed to avoid returning raw Message which matches Object
                return handlerMethod.invoke(message, message.getPayload(), originData, acknowledgment, connector, super.createMetadata(originData));
            return handlerMethod.invoke(message, originData, acknowledgment, connector, super.createMetadata(originData));
        } else if (handlerMethod.equals(this.defaultHandlerMethod))
            // Needed to avoid returning raw Message which matches Object
            return handlerMethod.invoke(message, message.getPayload(), originData, acknowledgment, connector);
        else
            return handlerMethod.invoke(message, originData, acknowledgment, connector);
    }

    @Override
    protected Type getParameterPayloadType(InvocableHandlerMethod handlerMethod, Type... exceptedTypes) {
        final Type[] types = Arrays.copyOf(exceptedTypes, exceptedTypes.length + 2);
        types[exceptedTypes.length] = this.rawEntry ? RAW_COLLECTION_TYPE : ENTRY_COLLECTION_TYPE;
        types[exceptedTypes.length + 1] = CanalEntries.class;
        return super.getParameterPayloadType(handlerMethod, types);
    }

    /**
     * {@inheritDoc}
     *
     * @return NONE if Collection or Collection<?> or Message or Message<?>
     */
    @Override
    protected ResolvableType doExtractParameterPayloadType(Type parameterType) {
        // Collection<CanalEntry.Entry> or Collection<ByteString>
        if (super.isSimilarType(parameterType, !this.rawEntry ? RAW_COLLECTION_TYPE : ENTRY_COLLECTION_TYPE))
            return ResolvableType.forType(parameterType).asCollection().getGeneric();
        final ResolvableType collectionType = ResolvableType.forType(parameterType).asCollection();
        // Collection<?>
        if (collectionType.hasUnresolvableGenerics())
            return ResolvableType.NONE;
        final ResolvableType messagesType = collectionType.getGeneric()
                .as(Message.class);
        // Collection<Message<?>>
        if (messagesType.hasUnresolvableGenerics())
            return ResolvableType.NONE;
        // Collection<Message<PayloadType>>
        final ResolvableType generic = messagesType.getGeneric();
        if (generic != ResolvableType.NONE)
            return generic;
        return super.doExtractParameterPayloadType(parameterType);
    }

}
