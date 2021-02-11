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
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.LogFactory;

import org.springframework.canal.annotation.CanalAfter;
import org.springframework.canal.annotation.CanalBefore;
import org.springframework.canal.annotation.listener.CanalHandler;
import org.springframework.canal.listener.CanalListenerErrorHandler;
import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.TypeUtils;
import org.springframework.canal.support.converter.record.DelegatingEachCanalMessageConverter;
import org.springframework.canal.support.converter.record.EachCanalMessageConverter;
import org.springframework.canal.support.converter.record.EachEntryCanalMessageConverter;
import org.springframework.canal.support.converter.record.EachRawCanalMessageConverter;
import org.springframework.canal.support.expression.AbstractExpressionResolver;
import org.springframework.canal.support.expression.UnmodifiableScope;
import org.springframework.core.MethodParameter;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessagingException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.util.StringUtils;

import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @param <T> the message converter's generic type
 * @author 橙子
 * @since 2020/12/8
 */
// todo: Implements SeekAwareListener for set offset
public abstract class AbstractMessageListenerAdapter<T> extends AbstractExpressionResolver<UnmodifiableScope> {
    protected static final Message<?> NULL_MESSAGE = MessageBuilder.withPayload(ByteString.EMPTY).build();
    private static final Type[] DEFAULT_EXCEPTED_TYPES = {Acknowledgment.class, CanalConnector.class, EntryMetadata.class};
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
    protected final InvocableHandlerMethod defaultHandlerMethod;
    protected final CanalListenerErrorHandler errorHandler;
    private final Map<InvocableHandlerMethod, MethodParameter> payloadParameterCache = new ConcurrentHashMap<>();
    private final Map<InvocableHandlerMethod, Type> payloadTypeCache = new ConcurrentHashMap<>();
    private final Map<InvocableHandlerMethod, Boolean> ackRequiredCache = new ConcurrentHashMap<>();
    private final Map<InvocableHandlerMethod, Boolean> metadataRequiredCache = new ConcurrentHashMap<>();
    private final Object bean;
    protected EachCanalMessageConverter<T> eachMessageConverter = new DelegatingEachCanalMessageConverter<>(new EachEntryCanalMessageConverter(), new EachRawCanalMessageConverter());
    private Type fallbackPayloadType = Object.class;
    private List<InvocableHandlerMethod> otherHandlerMethods;

    protected AbstractMessageListenerAdapter(Object bean, InvocableHandlerMethod defaultHandlerMethod, CanalListenerErrorHandler errorHandler) {
        this(bean, defaultHandlerMethod, defaultHandlerMethod == null ? new ArrayList<>() : null, errorHandler);
    }

    protected AbstractMessageListenerAdapter(Object bean, InvocableHandlerMethod defaultHandlerMethod, List<InvocableHandlerMethod> otherHandlerMethods, CanalListenerErrorHandler errorHandler) {
        super(new UnmodifiableScope());
        this.bean = bean;
        this.defaultHandlerMethod = defaultHandlerMethod;
        this.otherHandlerMethods = otherHandlerMethods;
        this.errorHandler = errorHandler;
    }

    public EachCanalMessageConverter<T> getEachMessageConverter() {
        return this.eachMessageConverter;
    }

    public void setEachMessageConverter(EachCanalMessageConverter<T> eachMessageConverter) {
        this.eachMessageConverter = eachMessageConverter;
    }

    public void setFallbackPayloadType(Type fallbackPayloadType) {
        this.fallbackPayloadType = fallbackPayloadType;
    }

    public void addHandlerMethod(InvocableHandlerMethod handlerMethod) {
        if (this.otherHandlerMethods == null)
            this.otherHandlerMethods = new ArrayList<>();
        this.otherHandlerMethods.add(handlerMethod);
    }

    public boolean isConversionRequired(InvocableHandlerMethod handlerMethod, Type... exceptedTypes) {
        return handlerMethod.getMethodParameters().length != getParameterSimilarTypeCount(
                handlerMethod,
                appendExceptedTypes(exceptedTypes));
    }

    public boolean isMetadataRequired(InvocableHandlerMethod handlerMethod) {
        return this.metadataRequiredCache.computeIfAbsent(handlerMethod,
                method -> getParameterSimilarTypeCount(method, EntryMetadata.class) > 0);
    }

    private boolean isAckRequired(InvocableHandlerMethod handlerMethod) {
        return this.ackRequiredCache.computeIfAbsent(handlerMethod,
                method -> getParameterSimilarTypeCount(method, Acknowledgment.class) > 0);
    }

    /**
     * @param exceptedTypes the types wants to excepted.
     *                      <strong>DO NOT WRAPPED WITH METHOD {@link #appendExceptedTypes(Type...)} WITCH WILL AUTO INVOKED IN THIS METHOD.</strong>
     */
    protected Type getParameterPayloadType(InvocableHandlerMethod handlerMethod, Type... exceptedTypes) {
        return this.payloadTypeCache.computeIfAbsent(handlerMethod, method -> {
            final MethodParameter payloadParameter = findPayloadParameter(method, exceptedTypes);
            return payloadParameter == null ? this.fallbackPayloadType : extractParameterPayloadType(payloadParameter);
        });
    }

    /**
     * Subclasses can override to use a different mechanism to determine the target type of the payload conversion.
     *
     * @param exceptedTypes the types wants to excepted.
     *                      <strong>DO NOT WRAPPED WITH METHOD {@link #appendExceptedTypes(Type...)} WITCH WILL AUTO INVOKED IN THIS METHOD.</strong>
     * @return null if Message or Message<?>
     */
    protected Type extractParameterPayloadType(MethodParameter parameter, Type... exceptedTypes) {
        final Type parameterType = parameter.getGenericParameterType();
        /*
         * We're looking for a single non-annotated parameter, or one annotated with @Payload, @CanalBefore, @CanalAfter.
         * We ignore parameters with type Acknowledgment, CanalConnector, EntryMetadata, ...exceptedTypes
         * because they are not involved with conversion.
         */
        if ((parameter.hasParameterAnnotations()
                && !parameter.hasParameterAnnotation(Payload.class)
                && !parameter.hasParameterAnnotation(CanalBefore.class)
                && !parameter.hasParameterAnnotation(CanalAfter.class))
                || isSimilarType(parameterType, appendExceptedTypes(exceptedTypes)))
            return null;
        final ResolvableType resolvableType = doExtractParameterPayloadType(parameterType);
        if (resolvableType == ResolvableType.NONE)
            return null;
        final Type payloadType = resolvableType.getType();
        if (payloadType instanceof WildcardType)
            return TypeUtils.getBothBounds((WildcardType) payloadType)[0][0];
        if (payloadType instanceof TypeVariable) {
            final Type[] bounds = TypeUtils.getBounds((TypeVariable<?>) payloadType);
            // Just in case Message<T extends C & I>
            if (bounds.length != 1)
                this.logger.warn(() -> "The bounds of " + parameter.toString() + " should be one but " + bounds.length + " found." +
                        " Select the first.");
            return bounds[0];
        }
        return payloadType;
    }

    /**
     * @see #extractParameterPayloadType(MethodParameter, Type...)
     */
    protected ResolvableType doExtractParameterPayloadType(Type parameterType) {
        final ResolvableType resolvableType = ResolvableType.forType(parameterType);
        final ResolvableType messageType = resolvableType.as(Message.class);
        if (messageType == ResolvableType.NONE)
            return resolvableType;
        // Message<?>
        if (messageType.hasUnresolvableGenerics())
            return ResolvableType.NONE;
        // Message<PayloadType>
        return messageType.getGeneric();
    }

    protected void handleException(Message<?> message, Object originData, CanalConnector connector, CanalException e) {
        if (this.errorHandler == null)
            throw e;
        try {
            if (NULL_MESSAGE.equals(message))
                message = MessageBuilder.withPayload(originData).build();
            this.errorHandler.handleError(message, e, connector);
        } catch (Exception exception) {
            throw new CanalException(createErrorCode("Listener error handler threw an exception for the incoming message",
                    originData), exception); // NOSONAR CanalClientException's stack trace loss
        }
    }

    protected final Object invokeHandlerMethod(InvocableHandlerMethod handlerMethod, Message<?> message, Object originData, Acknowledgment acknowledgment, CanalConnector connector) {
        try {
            return doInvokeHandlerMethod(handlerMethod, message, originData, acknowledgment, connector);
        } catch (MessageConversionException e) {
            if (isAckRequired(handlerMethod) && acknowledgment == null)
                throw new CanalException("invokeHandler Failed", new IllegalStateException("No Acknowledgment available as an argument, "
                        + "the listener container must have a MANUAL AckMode to populate the Acknowledgment.", e));
            throw new CanalException(createErrorCode("Listener method could not be invoked with the incoming message", originData), e);
        } catch (MessagingException e) {
            throw new CanalException(createErrorCode("Listener method could not be invoked with the incoming message", originData), e);
        } catch (Exception e) {
            throw new CanalException(createErrorCode("Listener method threw exception", originData), e);
        }
    }

    protected Object doInvokeHandlerMethod(InvocableHandlerMethod handlerMethod, Message<?> message, Object originData, Acknowledgment acknowledgment, CanalConnector connector) throws Exception {
        if (isMetadataRequired(handlerMethod))
            return handlerMethod.invoke(message, originData, acknowledgment, connector, createMetadata(originData));
        else
            return handlerMethod.invoke(message, originData, acknowledgment, connector);
    }

    protected final InvocableHandlerMethod findHandlerMethod(Object originData) {
        if (this.defaultHandlerMethod != null)
            return this.defaultHandlerMethod;
        InvocableHandlerMethod payloadHandlerMethod = null;
        for (InvocableHandlerMethod handlerMethod : this.otherHandlerMethods) {
            final CanalHandler canalHandler = handlerMethod.getMethodAnnotation(CanalHandler.class);
            final String filter = (String) AnnotationUtils.getValue(canalHandler, "filter");
            final String recordRef = canalHandler == null ? null : canalHandler.recordRef();
            if (StringUtils.hasText(recordRef))
                getExpressionScope().addBean(recordRef, originData);
            boolean foundCandidate = !StringUtils.hasText(filter) || Boolean.TRUE.equals(resolveExpressionAsBoolean(filter, "filter"));
            if (StringUtils.hasText(recordRef))
                getExpressionScope().removeBean(recordRef);
            if (foundCandidate) {
                if (payloadHandlerMethod != null)
                    throw new CanalException("Ambiguous methods for record: " + originData + ": " +
                            payloadHandlerMethod.getMethod().getName() + " and " + handlerMethod.getMethod().getName());
                payloadHandlerMethod = handlerMethod;
            }
        }
        return payloadHandlerMethod;
    }

    /**
     * Iterate whole parameters.
     */
    protected final int getParameterSimilarTypeCount(InvocableHandlerMethod handlerMethod, Type... expectedTypes) {
        return getParameterSimilarTypeCount(handlerMethod, new Type[0], expectedTypes);
    }

    /**
     * Stop count if iterate to the parameter(also included) with payload type.
     *
     * @param exceptedPayloadTypes the parameter with payload type wants to excepted.
     *                             <strong>DO NOT WRAPPED WITH METHOD {@link #appendExceptedTypes(Type...)} WITCH WILL AUTO INVOKED IN THIS METHOD.</strong>
     * @see #findPayloadParameter(InvocableHandlerMethod, Type...)
     */
    protected final int getParameterSimilarTypeCount(InvocableHandlerMethod handlerMethod, Type[] exceptedPayloadTypes, Type... expectedTypes) {
        final MethodParameter payloadParameter;
        if (exceptedPayloadTypes.length == 0)
            payloadParameter = null;
        else {
            payloadParameter = findPayloadParameter(handlerMethod, exceptedPayloadTypes);
            if (payloadParameter == null)
                return 0;
        }
        int count = 0;
        for (MethodParameter parameter : handlerMethod.getMethodParameters()) {
            if (isSimilarType(parameter.getGenericParameterType(), expectedTypes))
                count++;
            if (parameter.equals(payloadParameter))
                break;
        }
        return count;
    }

    protected final Type[] appendExceptedTypes(Type... exceptedTypes) {
        if (exceptedTypes.length == 0)
            return DEFAULT_EXCEPTED_TYPES;
        final Type[] types = Arrays.copyOf(DEFAULT_EXCEPTED_TYPES, DEFAULT_EXCEPTED_TYPES.length + exceptedTypes.length);
        System.arraycopy(exceptedTypes, 0, types, DEFAULT_EXCEPTED_TYPES.length, exceptedTypes.length);
        return types;
    }

    /**
     * @return For example, when the type is {@code Message}
     * return if the expected types contain {@code Message}.
     * <p>
     * When the type is {@code Message<?>}
     * return if the expected types contain {@code Message} or {@code Message<?>}.
     * <p>
     * When the type is {@code Message<T>}
     * return if the expected types contain {@code Message}, {@code Message<?>} or {@code Message<T>}.
     * <p>
     * When the type is {@code List<Message>}
     * return if the expected types contain {@code List},
     * {@code Collection<Message>} or {@code List<Message>}.
     * <p>
     * When the type is {@code List<Message<?>>}
     * return if the expected types contain {@code List},
     * {@code Collection<Message>}, {@code List<Message>},
     * {@code Collection<Message<?>>}, {@code List<Message<?>>}, {@code Collection<Message<T>>} or {@code List<Message<T>>}.
     * <p>
     * When the type is {@code List<? extends Message<?>>}
     * return if the expected types contain {@code List},
     * {@code Collection<Message>}, {@code Collection<Message<?>>},
     * {@code List<Message>}, {@code List<Message<?>>},
     * {@code Collection<? extends Message>}, {@code Collection<? extends Message<?>>},
     * {@code List<? extends Message>} or {@code List<? extends Message<?>>}.
     * <p>
     * When the type is {@code List<? extends Message<T>>}
     * return if the expected types contain {@code List},
     * {@code Collection<Message>}, {@code Collection<Message<?>>}, {@code Collection<Message<T>>},
     * {@code List<Message>}, {@code List<Message<?>>}, {@code List<Message<T>>},
     * {@code Collection<? extends Message>}, {@code Collection<? extends Message<?>>}, {@code Collection<? extends Message<T>>},
     * {@code List<? extends Message>}, {@code List<? extends Message<?>>} or {@code List<? extends Message<T>>}.
     * <p>
     * When the type is {@code List<T extends Message<T>>}
     * return if the expected types contain {@code List},
     * {@code Collection<Message>}, {@code Collection<Message<?>>}, {@code Collection<Message<T>>},
     * {@code List<Message>}, {@code List<Message<?>>}, {@code List<Message<T>>},
     * {@code Collection<? extends Message>}, {@code Collection<? extends Message<?>>}, {@code Collection<? extends Message<T>>},
     * {@code List<? extends Message>}, {@code List<? extends Message<?>>}, {@code List<? extends Message<T>>},
     * {@code Collection<T extends Message>}, {@code Collection<T extends Message<?>>}, {@code Collection<T extends Message<T>>},
     * {@code List<T extends Message>}, {@code List<T extends Message<?>>} or {@code List<T extends Message<T>>}.
     */
    protected final boolean isSimilarType(Type type, Type... expectedTypes) {
        for (Type expectType : expectedTypes)
            if (TypeUtils.isAssignable(expectType, type))
                return true;
        return false;
    }

    protected EntryMetadata createMetadata(Object data) {
        CanalEntry.Entry entry;
        if (data instanceof CanalEntry.Entry)
            entry = (CanalEntry.Entry) data;
        else if (data instanceof ByteString)
            try {
                entry = CanalEntry.Entry.parseFrom((ByteString) data);
            } catch (InvalidProtocolBufferException e) {
                return null;
            }
        else
            return null;
        return new EntryMetadata(entry.getEntryType(), entry.getUnknownFields(), entry.getSerializedSize());
    }

    protected final void reset() {
        this.payloadTypeCache.clear();
        this.payloadParameterCache.clear();
        this.ackRequiredCache.clear();
        this.metadataRequiredCache.clear();
    }

    /**
     * @param exceptedTypes the types wants to excepted.
     *                      <strong>DO NOT WRAPPED WITH METHOD {@link #appendExceptedTypes(Type...)} WITCH WILL AUTO INVOKED IN THIS METHOD.</strong>
     * @return Parameter with payload type extracted by method {@link #extractParameterPayloadType(MethodParameter, Type...)};
     * null if no payload type extracted,
     * else the result will be cached <strong>no matter excepted types changed</strong>.
     */
    private MethodParameter findPayloadParameter(InvocableHandlerMethod handlerMethod, Type... exceptedTypes) {
        return this.payloadParameterCache.computeIfAbsent(handlerMethod, method -> {
            MethodParameter payloadParameter = null;
            for (MethodParameter parameter : method.getMethodParameters())
                if (extractParameterPayloadType(parameter, exceptedTypes) != null) {
                    if (payloadParameter != null) {
                        this.logger.debug(() -> "Ambiguous parameters for target payload for listenerMethod " + method.getMethod()
                                + "; no inferred type available");
                        break;
                    }
                    payloadParameter = parameter;
                }
            return payloadParameter;
        });
    }

    private String createErrorCode(String description, Object data) {
        return description + "\n"
                + "Endpoint handler details:\n"
                + "Method [" + findHandlerMethod(data).getMethod().toGenericString() + "]\n"
                + "Bean [" + this.bean + "]";
    }
}
