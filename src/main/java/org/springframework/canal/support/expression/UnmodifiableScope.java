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

import org.springframework.beans.factory.ObjectFactory;
import org.springframework.beans.factory.config.BeanExpressionContext;
import org.springframework.beans.factory.config.Scope;

import java.util.HashMap;
import java.util.Map;

/**
 * Add to {@link BeanExpressionContext}, in order to use bean refs.
 *
 * @author 橙子
 * @since 2020/12/11
 */
public class UnmodifiableScope implements Scope {
    private final Map<String, Object> beans = new HashMap<>();

    public void addBean(String key, Object bean) {
        this.beans.put(key, bean);
    }

    public void removeBean(String key) {
        this.beans.remove(key);
    }

    @Override
    public Object get(String name, ObjectFactory<?> objectFactory) {
        return this.beans.get(name);
    }

    @Override
    public Object remove(String name) {
        return null;
    }

    @Override
    public void registerDestructionCallback(String name, Runnable callback) {
        // Do nothing because of unmodifiable.
    }

    @Override
    public Object resolveContextualObject(String key) {
        return this.beans.get(key);
    }

    @Override
    public String getConversationId() {
        return null;
    }
}
