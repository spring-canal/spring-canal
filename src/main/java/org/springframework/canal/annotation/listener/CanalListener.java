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

package org.springframework.canal.annotation.listener;

import org.springframework.canal.annotation.CanalListenerAnnotationBeanPostProcessor;
import org.springframework.canal.annotation.EnableCanal;
import org.springframework.canal.config.endpoint.CanalListenerMethodEndpoint;
import org.springframework.canal.config.endpoint.register.CanalListenerEndpointRegistry;
import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;
import org.springframework.messaging.handler.annotation.MessageMapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that marks a method to be the target of a Canal message listener on the
 * specified subscribes.
 * <p>
 * The {@link #containerFactory()} identifies the
 * {@link CanalListenerContainerFactory CanalListenerContainerFactory}
 * to use to build the Canal listener container;
 * if not set, a <em>default</em> container factory is assumed to be available with a bean name
 * of {@code CanalListenerContainerFactory} unless an explicit default has been provided through configuration.
 * <p>
 * Processing of {@code @CanalListener} annotations is performed by registering a
 * {@link CanalListenerAnnotationBeanPostProcessor}. This can be done manually or, more
 * conveniently, through {@link EnableCanal} annotation.
 * <p>
 * Annotated methods are allowed to have flexible signatures similar to what
 * {@link MessageMapping} provides, that is
 * <ul>
 * <li>{@link com.alibaba.otter.canal.protocol.Message} to access to the raw Canal
 * message</li>
 * <li>{@link org.springframework.canal.support.Acknowledgment Acknowledgment} to manually ack</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Payload @Payload}-annotated
 * method arguments including the support of validation</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Header @Header}-annotated
 * method arguments to extract a specific header value, defined by
 * {@link CanalMessageHeaderAccessor CanalMessageHeaderAccessor}</li>
 * <li>{@link org.springframework.messaging.handler.annotation.Headers @Headers}-annotated
 * argument that must also be assignable to {@link java.util.Map} for getting access to
 * all headers.</li>
 * <li>{@link org.springframework.messaging.MessageHeaders MessageHeaders} arguments for
 * getting access to all headers.</li>
 * <li>{@link org.springframework.messaging.support.MessageHeaderAccessor
 * MessageHeaderAccessor} for convenient access to all method arguments.</li>
 * </ul>
 *
 * <p>When defined at the method level, a listener container is created for each method.
 * The {@link org.springframework.canal.listener.MessageListener MessageListener} is a
 * {@link org.springframework.canal.listener.adapter.EachMessageListenerAdapter EachMessageListenerAdapter} or
 * {@link org.springframework.canal.listener.adapter.BatchMessageListenerAdapter BatchMessageListenerAdapter},
 * configured with a {@link CanalListenerMethodEndpoint}.
 *
 * <p>When defined at the class level, a listener container is used to
 * service all methods annotated with {@code @CanalHandler}. Method signatures of such
 * annotated methods must not cause any ambiguity such that a single method can be
 * resolved for a particular inbound message. The
 * {@link org.springframework.canal.listener.adapter.EachMessageListenerAdapter EachMessageListenerAdapter} or
 * {@link org.springframework.canal.listener.adapter.BatchMessageListenerAdapter BatchMessageListenerAdapter} is
 * configured with a
 * {@link org.springframework.canal.config.endpoint.CanalListenerClassEndpoint CanalListenerClassEndpoint}.
 *
 * @author 橙子
 * @see EnableCanal
 * @see CanalListenerAnnotationBeanPostProcessor
 * @see CanalListeners
 * @since 2020/11/14
 */
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
@Retention(RetentionPolicy.RUNTIME)
@MessageMapping
@Documented
@Repeatable(CanalListener.CanalListeners.class)
public @interface CanalListener {

    /**
     * The unique identifier of the container managing for this endpoint.
     * <p>If none is specified an auto-generated one is provided.
     * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
     *
     * @return the {@code id} for the container managing for this endpoint.
     * @see CanalListenerEndpointRegistry#getListenerContainer(String)
     */
    String id() default "";

    String containerFactory() default "";

    /**
     * @return the subscribe name to subscribe to.
     */
    String[] subscribes() default {".*\\..*"};

    /**
     * If provided, the listener container for this listener will be added to a bean
     * with this value as its name, of type {@code Collection<MessageListenerContainer>}.
     * This allows, for example, iteration over the collection to start/stop a subset
     * of containers.
     * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
     *
     * @return the bean name for the group.
     */
    String containerGroup() default "";

    /**
     * Set an {@link org.springframework.canal.listener.CanalListenerErrorHandler} bean
     * name to invoke if the listener method throws an exception.
     *
     * @return the error handler.
     */
    String errorHandler() default "";

    /**
     * A pseudo bean name used in SpEL expressions within this annotation to reference
     * the current bean within which this listener is defined. This allows access to
     * properties and methods within the enclosing bean.
     * Default '__listener'.
     * <p>
     * Example: {@code subscribe = "#{__listener.topicList}"}.
     *
     * @return the pseudo bean name.
     */
    String beanRef() default "__listener";

    /**
     * Override the container factory's {@code concurrency} setting for this listener. May
     * be a property placeholder or SpEL expression that evaluates to a {@link Number}, in
     * which case {@link Number#intValue()} is used to obtain the value.
     * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
     *
     * @return the concurrency.
     */
    String concurrency() default "";

    /**
     * Set to true or false, to override the default setting in the container factory. May
     * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
     * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
     * obtain the value.
     * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
     *
     * @return true to auto start, false to not auto start.
     */
    String autoStartup() default "";

    /**
     * Set to true or false, to override the default setting in the container factory. May
     * be a property placeholder or SpEL expression that evaluates to a {@link Boolean} or
     * a {@link String}, in which case the {@link Boolean#parseBoolean(String)} is used to
     * obtain the value.
     * <p>SpEL {@code #{...}} and property place holders {@code ${...}} are supported.
     *
     * @return true to listen batch message, false to listen each message.
     */
    String batchListener() default "";

    /**
     * Canal consumer/container properties; they will supersede any properties with the same name
     * defined in the consumer/container factory (if the consumer/container factory supports property overrides).
     * <h3>Supported Syntax</h3>
     * <p>The supported syntax for key-value pairs is the same as the
     * syntax defined for entries in a Java
     * {@linkplain java.util.Properties#load(java.io.Reader) properties file}:
     * <ul>
     * <li>{@code key=value}</li>
     * <li>{@code key:value}</li>
     * <li>{@code key value}</li>
     * </ul>
     *
     * @return the properties.
     * @see org.springframework.canal.config.ContainerProperties
     * @see org.springframework.canal.config.ConsumerProperties
     */
    String[] properties() default {};

    // return type is always void, no need for splitIterables

    @Target({ElementType.TYPE, ElementType.METHOD, ElementType.ANNOTATION_TYPE})
    @Retention(RetentionPolicy.RUNTIME)
    @Documented
    @interface CanalListeners {
        CanalListener[] value();
    }
}
