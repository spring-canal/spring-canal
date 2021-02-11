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

package org.springframework.canal.listener;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.apache.commons.logging.LogFactory;

import org.springframework.canal.listener.adapter.DelegatingMessageListenerAdapter;
import org.springframework.core.log.LogAccessor;
import org.springframework.util.Assert;

/**
 * @author 橙子
 * @since 2020/12/9
 */
public final class ListenerUtils {
    private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ListenerUtils.class));

    private ListenerUtils() {
    }

    public static void subscribeFilters(CanalConnector connector, String subscribe, int retryTimes, long retryInterval, boolean autoRollback) {
        for (int i = 0; i < retryTimes; i++)
            try {
                ListenerUtils.connect(connector);
                if (subscribe == null)
                    connector.subscribe();
                else
                    connector.subscribe(subscribe);
                if (autoRollback)
                    connector.rollback();
                break;
            } catch (Exception e) {
                connector.disconnect();
                if (i == retryTimes - 1)
                    throw e;
                ListenerUtils.LOGGER.error(e, () -> "Could not subscribe to " + subscribe + ", We will try again later.");
                try {
                    Thread.sleep(retryInterval);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new CanalClientException(ex);
                }
            }
    }

    public static boolean parseBoolean(Object object) {
        return object instanceof String ? Boolean.parseBoolean((String) object) : (Boolean) object;
    }

    public static int parseInteger(Object object) {
        return object instanceof String ? Integer.parseInt((String) object) : (Integer) object;
    }

    public static long parseLong(Object object) {
        return object instanceof String ? Long.parseLong((String) object) : (Long) object;
    }

    public static ParameterType determineListenerParameterType(Object listener) {
        Assert.notNull(listener, "Listener must not be null");
        while (listener instanceof DelegatingMessageListenerAdapter)
            listener = ((DelegatingMessageListenerAdapter<?>) listener).getDelegate();
        if (listener instanceof EachAckConnectorAwareMessageListener || listener instanceof BatchAckConnectorAwareMessageListener)
            return ParameterType.ACK_CONNECTOR_AWARE;
        else if (listener instanceof EachConnectorAwareMessageListener || listener instanceof BatchConnectorAwareMessageListener)
            return ParameterType.CONNECTOR_AWARE;
        else if (listener instanceof EachAckMessageListener || listener instanceof BatchAckMessageListener)
            return ParameterType.ACK;
        else if (listener instanceof MessageListener)
            return ParameterType.SIMPLE;
        throw new IllegalArgumentException("Unsupported listener type: " + listener.getClass().getName());
    }

    private static RuntimeException connect(CanalConnector connector) {
        try {
            connector.connect();
//                    connector.unsubscribe()
            return null;
        } catch (CanalClientException e) {
            ListenerUtils.LOGGER.error(e, () -> "Failed to connect - check canal connection properties.");
            return e;
        }
    }

    public enum ParameterType {
        /**
         * Acknowledging and consumer aware.
         */
        ACK_CONNECTOR_AWARE,
        /**
         * Consumer aware.
         */
        CONNECTOR_AWARE,
        /**
         * Acknowledging.
         */
        ACK,
        /**
         * Simple.
         */
        SIMPLE;
    }
}
