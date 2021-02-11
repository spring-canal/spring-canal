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

import org.apache.commons.logging.LogFactory;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.canal.annotation.endpoint.register.CanalListenerEndpointRegistrarCustomizer;
import org.springframework.canal.annotation.listener.CanalHandler;
import org.springframework.canal.annotation.listener.CanalListener;
import org.springframework.canal.bootstrap.CanalBootstrapConfiguration;
import org.springframework.canal.config.endpoint.AbstractCanalListenerEndpoint;
import org.springframework.canal.config.endpoint.CanalListenerClassEndpoint;
import org.springframework.canal.config.endpoint.CanalListenerMethodEndpoint;
import org.springframework.canal.config.endpoint.register.CanalListenerEndpointRegistrar;
import org.springframework.canal.config.endpoint.register.CanalListenerEndpointRegistry;
import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.canal.listener.CanalListenerErrorHandler;
import org.springframework.canal.support.converter.message.CanalJpaMessageConverter;
import org.springframework.canal.support.expression.AbstractExpressionResolver;
import org.springframework.canal.support.expression.UnmodifiableScope;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.convert.converter.GenericConverter;
import org.springframework.core.log.LogAccessor;
import org.springframework.format.Formatter;
import org.springframework.format.FormatterRegistry;
import org.springframework.format.support.DefaultFormattingConversionService;
import org.springframework.format.support.FormattingConversionService;
import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.validation.Validator;

import javax.persistence.EntityManagerFactory;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This post-processor is automatically registered by Spring's {@link EnableCanal} annotation.
 *
 * @author 橙子
 * @since 2020/11/14
 */
public class CanalListenerAnnotationBeanPostProcessor extends AbstractExpressionResolver<UnmodifiableScope> implements SmartInitializingSingleton, BeanPostProcessor, Ordered {
    private static final String GENERATED_ID_PREFIX = "org.springframework.canal.CanalListenerEndpointContainer#";
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
    private final Set<Class<?>> nonAnnotatedClasses = new CopyOnWriteArraySet<>();
    private final AtomicInteger endpointCounter = new AtomicInteger();
    /**
     * Used for {@link CanalListenerEndpointRegistrar#afterPropertiesSet() register_listeners}.
     * <p>
     * Although {@link CanalListenerEndpointRegistrar} implements some {@code ...Aware},
     * it is manually created by default, so its override methods has to be explicitly configured.
     *
     * @see #afterSingletonsInstantiated()
     * @see #processListener(AbstractCanalListenerEndpoint, CanalListener, Object, Object, String)
     */
    private final CanalListenerEndpointRegistrar registrar = new CanalListenerEndpointRegistrar();
    /**
     * Temporarily stored field is passed to POJO use method {@link CanalListenerMethodEndpoint#setMessageHandlerMethodFactory(MessageHandlerMethodFactory)}.
     *
     * @see #processListener(AbstractCanalListenerEndpoint, CanalListener, Object, Object, String)
     */
    private final MessageHandlerMethodFactoryAdapter messageHandlerMethodFactoryAdapter = new MessageHandlerMethodFactoryAdapter();

    private BeanFactory beanFactory;
    /**
     * If set, this temporarily stored field is passed to {@link CanalListenerEndpointRegistrar#setEndpointRegistry(CanalListenerEndpointRegistry)}.
     *
     * @see #afterSingletonsInstantiated()
     */
    private CanalListenerEndpointRegistry endpointRegistry;
    /**
     * Temporarily stored field is passed to {@link CanalListenerEndpointRegistrar#setDefaultContainerFactoryBeanName(String)}.
     *
     * @see #afterSingletonsInstantiated()
     */
    private String defaultContainerFactoryBeanName = CanalBootstrapConfiguration.CANAL_LISTENER_CONTAINER_FACTORY_BEAN_NAME;
    /**
     * If need default {@link MessageHandlerMethodFactory}, this temporarily stored field is passed to {@link MessageHandlerMethodFactoryAdapter#createDefaultMessageHandlerMethodFactory()}.
     *
     * @see #messageHandlerMethodFactoryAdapter
     */
    private EntityManagerFactory entityManagerFactory;
    /**
     * If need default {@link MessageHandlerMethodFactory}, this temporarily stored field is passed to {@link MessageHandlerMethodFactoryAdapter#createDefaultMessageHandlerMethodFactory()}.
     *
     * @see #messageHandlerMethodFactoryAdapter
     */
    private Charset charset = StandardCharsets.UTF_8;

    /**
     * Add scope to {@link org.springframework.beans.factory.config.BeanExpressionContext expressionContext},
     * in order to use {@link CanalListener#beanRef() '__listener'}
     * to get the current bean within which the listener(method annotated with {@link CanalListener @CanalListener}) is defined.
     *
     * @see #setBeanFactory(BeanFactory)
     * @see #processListener(AbstractCanalListenerEndpoint, CanalListener, Object, Object, String)
     * @see CanalListener#beanRef()
     */
    public CanalListenerAnnotationBeanPostProcessor() {
        super(new UnmodifiableScope());
    }

    /**
     * Set {@link #registrar} using custom attributes.
     */
    @Override
    public void afterSingletonsInstantiated() {
        this.registrar.setBeanFactory(this.beanFactory);
        if (this.beanFactory instanceof ListableBeanFactory)
            for (CanalListenerEndpointRegistrarCustomizer customizer : ((ListableBeanFactory) this.beanFactory).getBeansOfType(CanalListenerEndpointRegistrarCustomizer.class).values())
                customizer.customize(this.registrar);
        // Set the value of registrar's defaultContainerFactoryBeanName to default if not customize
        if (this.registrar.getDefaultContainerFactoryBeanName() == null)
            this.registrar.setDefaultContainerFactoryBeanName(this.defaultContainerFactoryBeanName);
        // Set the value of registrar's CanalListenerEndpointRegistry to default if not customize
        if (this.registrar.getEndpointRegistry() == null) {
            if (this.endpointRegistry == null) {
                Assert.state(this.beanFactory != null, "BeanFactory must be set to find endpoint registry by bean name");
                this.endpointRegistry = this.beanFactory.getBean(CanalBootstrapConfiguration.CANAL_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME,
                        CanalListenerEndpointRegistry.class);
            }
            this.registrar.setEndpointRegistry(this.endpointRegistry);
        }
        // Wrapper set the custom MessageHandlerMethodFactory once resolved by the customizer
        MessageHandlerMethodFactory handlerMethodFactory = this.registrar.getMessageHandlerMethodFactory();
        if (handlerMethodFactory != null)
            this.messageHandlerMethodFactoryAdapter.setHandlerMethodFactory(handlerMethodFactory);
        else
            addFormatters(this.messageHandlerMethodFactoryAdapter.headerConversionService);
        // Actually register all listeners
        this.registrar.afterPropertiesSet();
    }

    /**
     * Find out the methods annotated with {@link CanalListener},
     * or the {@link CanalHandler} annotated methods in {@link CanalListener} annotated classes.
     */
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) {
        if (!this.nonAnnotatedClasses.contains(bean.getClass())) {
            final Class<?> targetClass = AopUtils.getTargetClass(bean);
            if (!processMethodLevelAnnotations(bean, beanName, targetClass))
                this.nonAnnotatedClasses.add(bean.getClass());
            processClassLevelAnnotations(bean, beanName, targetClass);
        }
        return bean;
    }

    /**
     * Making a {@link BeanFactory} available is optional; if not set,
     * {@link CanalListenerEndpointRegistrarCustomizer} beans won't get autodetected and an
     * {@link #setEndpointRegistry endpoint registry} has to be explicitly configured.
     *
     * @param beanFactory the {@link BeanFactory} to be used.
     */
    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        super.setBeanFactory(beanFactory);
        this.beanFactory = beanFactory;
        if (this.entityManagerFactory == null)
            this.entityManagerFactory = beanFactory.getBean(EntityManagerFactory.class);
    }

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    /**
     * Manually customize {@link CanalListenerEndpointRegistry endpointRegistry} for method {@link #afterSingletonsInstantiated()}.
     */
    public void setEndpointRegistry(CanalListenerEndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
    }

    /**
     * Manually customize {@code defaultContainerFactoryBeanName} for method {@link #afterSingletonsInstantiated()};
     * if not set, a <em>default</em> container factory is assumed to be available
     * with a bean name of {@link CanalBootstrapConfiguration#CANAL_LISTENER_CONTAINER_FACTORY_BEAN_NAME CanalListenerContainerFactory}.
     */
    public void setDefaultContainerFactoryBeanName(String defaultContainerFactoryBeanName) {
        this.defaultContainerFactoryBeanName = defaultContainerFactoryBeanName;
    }

    /**
     * Manually customize {@link EntityManagerFactory} for method {@link MessageHandlerMethodFactoryAdapter#createDefaultMessageHandlerMethodFactory()}.
     */
    public void setEntityManagerFactory(EntityManagerFactory entityManagerFactory) {
        this.entityManagerFactory = entityManagerFactory;
    }

    /**
     * Manually customize {@link Charset} for method {@link MessageHandlerMethodFactoryAdapter#createDefaultMessageHandlerMethodFactory()}.
     */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    private void addFormatters(FormatterRegistry registry) {
        if (this.beanFactory instanceof ListableBeanFactory) {
            for (Converter<?, ?> converter : ((ListableBeanFactory) this.beanFactory).getBeansOfType(Converter.class).values())
                registry.addConverter(converter);
            for (GenericConverter converter : ((ListableBeanFactory) this.beanFactory).getBeansOfType(GenericConverter.class).values())
                registry.addConverter(converter);
            for (org.springframework.format.Formatter<?> formatter : ((ListableBeanFactory) this.beanFactory).getBeansOfType(Formatter.class).values())
                registry.addFormatter(formatter);
        }
    }

    /**
     * Create method-level endpoint then register in {@link #processListener(AbstractCanalListenerEndpoint, CanalListener, Object, Object, String)}.
     */
    protected void processMethodCanalListener(CanalListener canalListener, Method method, Object bean, String beanName) {
        Method methodToUse = checkProxy(method, bean);
        processListener(new CanalListenerMethodEndpoint<>(methodToUse, bean), canalListener, bean, methodToUse, beanName);
    }

    /**
     * Create class-level endpoint then register in {@link #processListener(AbstractCanalListenerEndpoint, CanalListener, Object, Object, String)}.
     */
    protected void processClassCanalListeners(Collection<CanalListener> classLevelListeners, List<Method> multiMethods, Object bean, String beanName) {
        List<Method> methodsToUse = new ArrayList<>();
        Method defaultMethod = null;
        for (Method method : multiMethods) {
            Method methodToUse = checkProxy(method, bean);
            CanalHandler annotation = AnnotationUtils.findAnnotation(method, CanalHandler.class);
            if (annotation != null && annotation.isDefault()) {
                final Method toAssert = defaultMethod;
                Assert.state(toAssert == null, () -> "Only one @CanalHandler can be marked 'isDefault', found: " + toAssert + " and " + method);
                defaultMethod = methodToUse;
            }
            methodsToUse.add(methodToUse);
        }
        for (CanalListener classLevelListener : classLevelListeners)
            processListener(new CanalListenerClassEndpoint<>(methodsToUse, defaultMethod, bean), classLevelListener, bean, bean.getClass(), beanName);
    }

    /**
     * Check if the annotation {@link CanalListener @CanalListener} exists in proxy interface method.
     */
    private Method checkProxy(Method methodArg, Object bean) {
        Method method = methodArg;
        try {
            // Found a @CanalListener method on the target class for this JDK proxy ->
            // is it also present on the proxy itself?
            method = getProxiedMethod(method, bean);
        } catch (SecurityException e) {
            ReflectionUtils.handleReflectionException(e);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(String.format("@CanalListener method '%s' found on bean target class '%s', " +
                    "but not found in any interface(s) for bean JDK proxy. Either " +
                    "pull the method up to an interface or switch to subclass (CGLIB) " +
                    "proxies by setting proxy-target-class/proxyTargetClass " +
                    "attribute to 'true'", methodArg.getName(), methodArg.getDeclaringClass().getSimpleName()), e);
        }
        return method;
    }

    private Method getProxiedMethod(Method method, Object bean) throws NoSuchMethodException {
        if (AopUtils.isJdkDynamicProxy(bean)) {
            method = bean.getClass().getMethod(method.getName(), method.getParameterTypes());
            Class<?>[] proxiedInterfaces = ((Advised) bean).getProxiedInterfaces();
            for (Class<?> proxiedInterface : proxiedInterfaces)
                try {
                    method = proxiedInterface.getMethod(method.getName(), method.getParameterTypes());
                    break;
                } catch (NoSuchMethodException ignored) {
                    // NOSONAR
                }
        }
        return method;
    }

    private void processListener(AbstractCanalListenerEndpoint endpoint, CanalListener canalListener, Object bean, Object adminTarget, String beanName) {
        String beanRef = canalListener.beanRef();
        if (StringUtils.hasText(beanRef))
            getExpressionScope().addBean(beanRef, bean);
        endpoint.setMessageHandlerMethodFactory(this.messageHandlerMethodFactoryAdapter);
        endpoint.setId(getEndpointId(canalListener));
        endpoint.setSubscribes(resolveSubscribes(canalListener));
        String group = canalListener.containerGroup();
        if (StringUtils.hasText(group))
            endpoint.setGroup(resolveExpressionAsString(group, "group"));
        String concurrency = canalListener.concurrency();
        if (StringUtils.hasText(concurrency))
            endpoint.setConcurrency(resolveExpressionAsInteger(concurrency, "concurrency"));
        String autoStartup = canalListener.autoStartup();
        if (StringUtils.hasText(autoStartup))
            endpoint.setAutoStartup(resolveExpressionAsBoolean(autoStartup, "autoStartup"));
        String batchListener = canalListener.batchListener();
        if (StringUtils.hasText(batchListener))
            endpoint.setBatchListener(resolveExpressionAsBoolean(batchListener, "batchListener"));
        resolveSupersedeProperties(endpoint, canalListener.properties());
        endpoint.setBeanFactory(this.beanFactory);
        String errorHandlerBeanName = resolveExpressionAsString(canalListener.errorHandler(), "errorHandler");
        if (StringUtils.hasText(errorHandlerBeanName)) {
            Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain error handler by bean name");
            endpoint.setErrorHandler(this.beanFactory.getBean(errorHandlerBeanName, CanalListenerErrorHandler.class));
        }
        CanalListenerContainerFactory<?> containerFactory = null;
        String containerFactoryBeanName = resolveEmbedded(canalListener.containerFactory());
        if (StringUtils.hasText(containerFactoryBeanName)) {
            Assert.state(this.beanFactory != null, "BeanFactory must be set to obtain container factory by bean name");
            try {
                containerFactory = this.beanFactory.getBean(containerFactoryBeanName, CanalListenerContainerFactory.class);
            } catch (NoSuchBeanDefinitionException ex) {
                throw new BeanInitializationException("Could not register Canal listener endpoint on [" + adminTarget
                        + "] for bean " + beanName + ", no " + CanalListenerContainerFactory.class.getSimpleName()
                        + " with id '" + containerFactoryBeanName + "' was found in the application context", ex);
            }
        }
        this.registrar.registerEndpoint(endpoint, containerFactory);
        if (StringUtils.hasText(beanRef))
            getExpressionScope().removeBean(beanRef);
    }

    private boolean processMethodLevelAnnotations(Object bean, String beanName, Class<?> targetClass) {
        final Map<Method, Set<CanalListener>> annotatedMethods = MethodIntrospector.selectMethods(targetClass, (Method method) -> {
            Set<CanalListener> listenerMethods = findListenerAnnotations(method);
            return listenerMethods.isEmpty() ? null : listenerMethods;
        });
        if (annotatedMethods.isEmpty()) {
            this.logger.trace(() -> "No @CanalListener annotations found on bean type: " + bean.getClass());
            return false;
        }
        // Non-empty set of methods
        for (Map.Entry<Method, Set<CanalListener>> entry : annotatedMethods.entrySet())
            for (CanalListener listener : entry.getValue())
                processMethodCanalListener(listener, entry.getKey(), bean, beanName);
        this.logger.debug(() -> annotatedMethods.size() + " @CanalListener methods processed on bean '" + beanName + "': " + annotatedMethods);
        return true;
    }

    private void processClassLevelAnnotations(Object bean, String beanName, Class<?> targetClass) {
        final Collection<CanalListener> classLevelListeners = findListenerAnnotations(targetClass);
        if (!classLevelListeners.isEmpty())
            processClassCanalListeners(classLevelListeners, new ArrayList<>(MethodIntrospector.selectMethods(targetClass, (Method method) ->
                    AnnotationUtils.findAnnotation(method, CanalHandler.class) != null)), bean, beanName);
    }

    /**
     * AnnotationUtils.getRepeatableAnnotations does not look at interfaces.
     */
    private Set<CanalListener> findListenerAnnotations(AnnotatedElement annotatedElement) {
        Set<CanalListener> listeners = new HashSet<>();
        CanalListener canalListener = AnnotatedElementUtils.findMergedAnnotation(annotatedElement, CanalListener.class);
        if (canalListener != null)
            listeners.add(canalListener);
        CanalListener.CanalListeners canalListeners = AnnotationUtils.findAnnotation(annotatedElement, CanalListener.CanalListeners.class);
        if (canalListeners != null)
            listeners.addAll(Arrays.asList(canalListeners.value()));
        return listeners;
    }

    private String getEndpointId(CanalListener canalListener) {
        if (StringUtils.hasText(canalListener.id()))
            return resolveExpressionAsString(canalListener.id(), "id");
        else
            return GENERATED_ID_PREFIX + this.endpointCounter.getAndIncrement();
    }

    private String[] resolveSubscribes(CanalListener canalListener) {
        String[] subscribes = canalListener.subscribes();
        List<String> result = new ArrayList<>(subscribes.length);
        for (String subscribe : subscribes)
            result.addAll(resolveExpressionAsStrings(subscribe, "subscribes"));
        return result.toArray(new String[0]);
    }

    private void resolveSupersedeProperties(AbstractCanalListenerEndpoint endpoint, String[] propertyStrings) {
        final Properties properties = new Properties();
        for (String property : propertyStrings)
            try {
                properties.load(new StringReader(property));
            } catch (IOException e) {
                this.logger.error(e, () -> "Failed to load property " + property + ", continuing...");
            }
        endpoint.setSupersedeProperties(properties);
    }

    /**
     * Adapter for interface {@link MessageHandlerMethodFactory} which suitable for Canal.
     */
    private final class MessageHandlerMethodFactoryAdapter implements MessageHandlerMethodFactory {
        private final FormattingConversionService headerConversionService;
        private MessageHandlerMethodFactory handlerMethodFactory;

        private MessageHandlerMethodFactoryAdapter() {
            this.headerConversionService = new DefaultFormattingConversionService();
            this.headerConversionService.addConverter(new BytesToString(CanalListenerAnnotationBeanPostProcessor.this.charset));
        }

        public void setHandlerMethodFactory(MessageHandlerMethodFactory handlerMethodFactory) {
            this.handlerMethodFactory = handlerMethodFactory;
        }

        @Override
        public InvocableHandlerMethod createInvocableHandlerMethod(Object bean, Method method) {
            if (this.handlerMethodFactory == null)
                this.handlerMethodFactory = createDefaultMessageHandlerMethodFactory();
            return this.handlerMethodFactory.createInvocableHandlerMethod(bean, method);
        }

        private MessageHandlerMethodFactory createDefaultMessageHandlerMethodFactory() {
            DefaultMessageHandlerMethodFactory defaultFactory = new CanalMessageHandlerMethodFactory();
            Validator validator = CanalListenerAnnotationBeanPostProcessor.this.registrar.getValidator();
            if (validator != null)
                defaultFactory.setValidator(validator);
            defaultFactory.setBeanFactory(CanalListenerAnnotationBeanPostProcessor.this.beanFactory);
            // Support parameter with/without annotation '@Header'
            defaultFactory.setConversionService(this.headerConversionService);
            MessageConverter payloadMessageConverter = new CanalJpaMessageConverter(
                    CanalListenerAnnotationBeanPostProcessor.this.entityManagerFactory,
                    true);
            // Support parameter type 'Message<PayloadType>'
            defaultFactory.setMessageConverter(payloadMessageConverter);

            List<HandlerMethodArgumentResolver> customArgumentsResolver = new ArrayList<>(
                    CanalListenerAnnotationBeanPostProcessor.this.registrar.getCustomMethodArgumentResolvers());
            // Support parameter with/without annotation '@Payload'
            // Has to be at the end - look at PayloadMethodArgumentResolver documentation
            customArgumentsResolver.add(new CanalPayloadMethodArgumentResolver(payloadMessageConverter, validator, false));
            defaultFactory.setCustomArgumentResolvers(customArgumentsResolver);

            defaultFactory.afterPropertiesSet();

            return defaultFactory;
        }
    }

    private static final class BytesToString implements Converter<byte[], String> {
        private final Charset charset;

        private BytesToString(Charset charset) {
            this.charset = charset;
        }

        @Override
        public String convert(byte[] source) {
            return new String(source, this.charset);
        }
    }
}
