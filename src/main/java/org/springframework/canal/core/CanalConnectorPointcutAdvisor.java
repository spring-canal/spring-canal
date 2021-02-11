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

package org.springframework.canal.core;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import org.springframework.aop.ClassFilter;
import org.springframework.aop.Pointcut;
import org.springframework.aop.support.AbstractBeanFactoryPointcutAdvisor;
import org.springframework.aop.support.StaticMethodMatcherPointcut;
import org.springframework.canal.support.CanalMessageUtils;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 橙子
 * @since 2020/11/23
 */
public class CanalConnectorPointcutAdvisor extends AbstractBeanFactoryPointcutAdvisor {
    private final Map<Object, Long> batchIdMap = new ConcurrentHashMap<>();

    public CanalConnectorPointcutAdvisor() {
        setAdvice((MethodInterceptor) methodInvocation -> {
            final Object targetBean = methodInvocation.getThis();
            final String methodName = methodInvocation.getMethod().getName();
            if (methodName.startsWith("disconnect"))
                return CanalConnectorPointcutAdvisor.this.doDisconnect(methodInvocation, targetBean);
            else if (methodName.startsWith("ack"))
                return CanalConnectorPointcutAdvisor.this.doAck(methodInvocation, targetBean);
            else if (Message.class.isAssignableFrom(methodInvocation.getMethod().getReturnType()))
                return CanalConnectorPointcutAdvisor.this.doGet(methodInvocation, targetBean);
            return methodInvocation.proceed();
        });
    }

    @Override
    public Pointcut getPointcut() {
        return new StaticMethodMatcherPointcut() {
            @Override
            public ClassFilter getClassFilter() {
                return CanalConnector.class::isAssignableFrom;
            }

            @Override
            public boolean matches(Method method, Class<?> targetClass) {
                return Message.class.equals(method.getReturnType())
                        || method.getName().startsWith("ack") || method.getName().startsWith("disconnect");
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        if (!super.equals(o))
            return false;
        return this.batchIdMap.equals(((CanalConnectorPointcutAdvisor) o).batchIdMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), this.batchIdMap);
    }

    protected Object doGet(MethodInvocation methodInvocation, Object targetBean) throws Throwable {
        final Object result = methodInvocation.proceed();
        if (result instanceof Message) {
            final long id = ((Message) result).getId();
            if (CanalMessageUtils.INVALID_MESSAGE_ID != id)
                CanalConnectorPointcutAdvisor.this.batchIdMap.put(targetBean, id);
        }
        return result;
    }

    protected Object doAck(MethodInvocation methodInvocation, Object targetBean) throws Throwable {
        final Object[] arguments = methodInvocation.getArguments();
        for (int i = 0; i < arguments.length; i++)
            if (CanalMessageUtils.INVALID_MESSAGE_ID.equals(arguments[i])) {
                final Long batchId = this.batchIdMap.remove(targetBean);
                if (batchId == null)
                    return null;
                arguments[i] = batchId;
                break;
            }
        return methodInvocation.proceed();
    }

    protected Object doDisconnect(MethodInvocation methodInvocation, Object targetBean) throws Throwable {
        this.batchIdMap.remove(targetBean);
        return methodInvocation.proceed();
    }
}
