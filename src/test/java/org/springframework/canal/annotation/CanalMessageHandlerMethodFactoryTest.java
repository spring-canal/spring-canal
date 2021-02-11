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

import org.junit.jupiter.api.Test;

import org.springframework.messaging.converter.MessageConverter;
import org.springframework.messaging.converter.SimpleMessageConverter;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author 橙子
 * @since 2021/1/1
 */
public class CanalMessageHandlerMethodFactoryTest {
    @Test
    void initArgumentResolvers() {
        final CanalMessageHandlerMethodFactory messageHandlerMethodFactory = new CanalMessageHandlerMethodFactory();
        messageHandlerMethodFactory.setMessageConverter(new SimpleMessageConverter());
        assertEquals(messageHandlerMethodFactory.initArgumentResolvers().size(), messageHandlerMethodFactory.initArgumentResolvers().size());
    }

    public static DefaultMessageHandlerMethodFactory getInstance(MessageConverter messageConverter) {
        final DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = new CanalMessageHandlerMethodFactory();
        messageHandlerMethodFactory.setMessageConverter(messageConverter);
        messageHandlerMethodFactory.setCustomArgumentResolvers(
                Collections.singletonList(new CanalPayloadMethodArgumentResolver(messageConverter, null, false)));
        return messageHandlerMethodFactory;
    }
}
