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

package org.springframework.canal.support.expression;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.BeanExpressionResolver;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.Scope;
import org.springframework.context.expression.StandardBeanExpressionResolver;

import java.util.ArrayList;
import java.util.List;

/**
 * Make sure {@link org.springframework.util.StringUtils#hasText(String)} is true before resolve expressions.
 *
 * @param <T> the scope type add to expression context
 * @author 橙子
 * @since 2020/12/11
 */
public abstract class AbstractExpressionResolver<T extends Scope> implements BeanFactoryAware {
    private final T expressionScope;
    private ConfigurableBeanFactory beanFactory;
    private BeanExpressionResolver resolver;
    private BeanExpressionContext expressionContext;

    protected AbstractExpressionResolver() {
        this(null);
    }

    protected AbstractExpressionResolver(T expressionScope) {
        this.expressionScope = expressionScope;
        setUpResolver();
    }

    public BeanExpressionResolver getResolver() {
        return this.resolver;
    }

    public BeanExpressionContext getExpressionContext() {
        return this.expressionContext;
    }

    public T getExpressionScope() {
        return this.expressionScope;
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        if (beanFactory instanceof ConfigurableListableBeanFactory) {
            this.beanFactory = (ConfigurableListableBeanFactory) beanFactory;
            setUpResolver();
        }
    }

    protected final List<String> resolveExpressionAsStrings(String expression, String attribute) {
        List<String> result = new ArrayList<>();
        try {
            resolveFlatted(resolveExpression(expression), result);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format("The [%s] must resolve to a String or an Iterable/Array" +
                            " that elements can be parsed as a String. Resolved to [%s] for [%s]",
                    attribute, expression.getClass(), expression), e);
        }
        return result;
    }

    protected final String resolveExpressionAsString(String expression, String attribute) {
        Object resolved = resolveExpression(expression);
        if (resolved instanceof String)
            return (String) resolved;
        throw new IllegalStateException(String.format("The [%s] must resolve to a String. Resolved to [%s] for [%s]",
                attribute, resolved == null ? null : resolved.getClass(), expression));
    }

    protected final boolean resolveExpressionAsBoolean(String expression, String attribute) {
        Object resolved = resolveExpression(expression);
        if (resolved instanceof String)
            return Boolean.parseBoolean((String) resolved);
        if (resolved instanceof Boolean)
            return (Boolean) resolved;
        throw new IllegalStateException(String.format("The [%s] must resolve to a Boolean or a String" +
                        " that can be parsed as a Boolean. Resolved to [%s] for [%s]",
                attribute, resolved == null ? null : resolved.getClass(), expression));
    }

    protected final int resolveExpressionAsInteger(String expression, String attribute) {
        Object resolved = resolveExpression(expression);
        if (resolved instanceof String)
            return Integer.parseInt((String) resolved);
        if (resolved instanceof Number)
            return ((Number) resolved).intValue();
        throw new IllegalStateException(String.format("The [%s] must resolve to an Number or a String" +
                        " that can be parsed as an Integer. Resolved to [%s] for [%s]",
                attribute, resolved == null ? null : resolved.getClass(), expression));
    }

    /**
     * Resolve the specified property if possible. such as {@code ${property-name}}
     *
     * @param property the property to resolve
     * @return the resolved property
     * @see ConfigurableBeanFactory#resolveEmbeddedValue
     */
    protected final String resolveEmbedded(String property) {
        if (this.beanFactory == null)
            return property;
        return this.beanFactory.resolveEmbeddedValue(property);
    }

    private Object resolveExpression(String expression) {
        return this.resolver.evaluate(resolveEmbedded(expression), this.expressionContext);
    }

    private void resolveFlatted(Object resolved, List<String> result) {
        if (resolved instanceof Iterable)
            for (Object object : (Iterable<?>) resolved)
                resolveFlatted(object, result);
        else if (resolved instanceof String[])
            for (Object object : (String[]) resolved)
                resolveFlatted(object, result);
        else if (resolved instanceof String)
            result.add((String) resolved);
        else
            throw new IllegalArgumentException(String.format("@CanalListener can't resolve '%s' as a String", resolved));
    }

    private void setUpResolver() {
        if (this.beanFactory == null) {
            this.resolver = new StandardBeanExpressionResolver();
            this.expressionContext = null;
        } else {
            this.resolver = this.beanFactory.getBeanExpressionResolver();
            this.expressionContext = new BeanExpressionContext(this.beanFactory, this.expressionScope);
        }
    }
}
