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

package org.springframework.canal.bootstrap;

import org.springframework.aop.config.AopConfigUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.canal.annotation.CanalListenerAnnotationBeanPostProcessor;
import org.springframework.canal.config.endpoint.register.CanalListenerEndpointRegistry;
import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.canal.core.CanalConnectorPointcutAdvisor;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.type.AnnotationMetadata;

/**
 * @author 橙子
 * @since 2020/11/14
 */
public class CanalBootstrapConfiguration implements ImportBeanDefinitionRegistrar {
    /**
     * The bean name of the default {@link CanalListenerContainerFactory}.
     */
    public static final String CANAL_LISTENER_CONTAINER_FACTORY_BEAN_NAME = "CanalListenerContainerFactory";
    /**
     * The bean name of the default {@link CanalListenerEndpointRegistry}.
     */
    public static final String CANAL_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME = "CanalListenerEndpointRegistry";
    private static final String CANAL_CONNECTOR_POINTCUT_ADVISOR_BEAN_NAME = "org.springframework.canal.core.internalCanalConnectorPointcutAdvisor";
    private static final String CANAL_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME = "org.springframework.canal.config.internalCanalListenerAnnotationProcessor";

    @Override
    public void registerBeanDefinitions(AnnotationMetadata importingClassMetadata, BeanDefinitionRegistry registry) {
        if (!registry.containsBeanDefinition(CANAL_CONNECTOR_POINTCUT_ADVISOR_BEAN_NAME)) {
            AopConfigUtils.registerAutoProxyCreatorIfNecessary(registry);
            final RootBeanDefinition advisorBeanDefinition = new RootBeanDefinition(CanalConnectorPointcutAdvisor.class);
            advisorBeanDefinition.setRole(BeanDefinition.ROLE_INFRASTRUCTURE);
            registry.registerBeanDefinition(CANAL_CONNECTOR_POINTCUT_ADVISOR_BEAN_NAME, advisorBeanDefinition);
        }
        if (!registry.containsBeanDefinition(CANAL_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME))
            registry.registerBeanDefinition(CANAL_LISTENER_ENDPOINT_REGISTRY_BEAN_NAME, new RootBeanDefinition(CanalListenerEndpointRegistry.class));
        if (!registry.containsBeanDefinition(CANAL_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME))
            registry.registerBeanDefinition(CANAL_LISTENER_ANNOTATION_PROCESSOR_BEAN_NAME, new RootBeanDefinition(CanalListenerAnnotationBeanPostProcessor.class));
    }
}
