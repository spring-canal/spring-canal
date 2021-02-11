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

package org.springframework.canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.logging.Logger;
import org.junit.platform.commons.logging.LoggerFactory;

import org.springframework.canal.config.ConsumerProperties;
import org.springframework.canal.core.SimpleCanalConnectorFactory;
import org.springframework.canal.listener.ListenerUtils;

import java.util.Collections;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * @author 橙子
 * @since 2021/2/9
 */
public class CanalBugTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(CanalBugTest.class);

    @Test
    void unsubscribe() throws InterruptedException {
        final ConsumerProperties consumerProperties = new ConsumerProperties();
        configConsumerProperties(consumerProperties);
        final SimpleCanalConnectorFactory connectorFactory = new SimpleCanalConnectorFactory(consumerProperties);
        final CanalConnector connector1 = connectorFactory.getConnector();
        final CanalConnector connector2 = connectorFactory.getConnector();
        final AtomicReference<Exception> thread1Exception = new AtomicReference<>();
        final AtomicReference<Exception> thread2Exception = new AtomicReference<>();
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
        final Thread thread1 = new Thread(() -> {
            try {
                ListenerUtils.subscribeFilters(connector1, null, 1, 0, true);
                cyclicBarrier.await();
                Thread.sleep(1000);
                LOGGER.info(() -> "Thread1 start get...");
                connector1.get(1);
            } catch (Exception e) {
                LOGGER.error(e, () -> "Thread1 exception");
                thread1Exception.set(e);
            } finally {
                connector1.disconnect();
            }
        });
        final Thread thread2 = new Thread(() -> {
            try {
                ListenerUtils.subscribeFilters(connector2, null, 1, 0, true);
                cyclicBarrier.await();
                LOGGER.info(() -> "Thread2 start unsubscribe...");
                connector2.unsubscribe();
            } catch (Exception e) {
                LOGGER.error(e, () -> "Thread2 exception");
                thread2Exception.set(e);
            } finally {
                connector2.disconnect();
            }
        });
        thread1.start();
        thread2.start();
        thread1.join();
        thread2.join();
        assertNull(thread2Exception.get());
        assertEquals(CanalClientException.class, thread1Exception.get().getClass());
    }

    private void configConsumerProperties(ConsumerProperties properties) {
        final String hostname = System.getProperty("canal.hostname");
        final String port = System.getProperty("canal.port");
        final String username = System.getProperty("canal.username");
        final String password = System.getProperty("canal.password");
        final String destination = System.getProperty("canal.destination");
        if (hostname != null && port != null) {
            properties.setHostname(Collections.singletonList(hostname));
            properties.setPort(Collections.singletonList(Integer.parseInt(port)));
        }
        properties.setUsername(username == null ? "canal" : username);
        properties.setPassword(password == null ? "canal" : password);
        properties.setDestination(destination == null ? "example" : destination);
    }
}
