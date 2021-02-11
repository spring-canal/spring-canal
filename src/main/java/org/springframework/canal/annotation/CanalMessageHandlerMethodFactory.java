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

import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;

import java.util.List;

/**
 * @author 橙子
 * @since 2020/12/31
 */
class CanalMessageHandlerMethodFactory extends DefaultMessageHandlerMethodFactory {
    private MessageConverter messageConverter;

    @Override
    public void setMessageConverter(MessageConverter messageConverter) {
        this.messageConverter = messageConverter;
        super.setMessageConverter(messageConverter);
    }

    @Override
    protected List<HandlerMethodArgumentResolver> initArgumentResolvers() {
        List<HandlerMethodArgumentResolver> resolvers = super.initArgumentResolvers();
        if (this.messageConverter != null)
            for (int i = 0; i < resolvers.size(); i++)
                if (resolvers.get(i) instanceof MessageMethodArgumentResolver) {
                    resolvers.add(i, new CanalMessageMethodArgumentResolver(this.messageConverter));
                    break;
                }
        return resolvers;
    }
}
