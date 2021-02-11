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

package org.springframework.canal.event;

import org.springframework.util.Assert;

/**
 * @author 橙子
 * @since 2020/12/6
 */
public class ListenerContainerNonResponsiveEvent extends CanalEvent {
    private final transient Object connector;
    private final long timeSinceLastFetch;
    private final String listenerId;
    private final String subscribe;

    public ListenerContainerNonResponsiveEvent(long timeSinceLastFetch, String id, String subscribe, Object connector,
                                               Object source, Object container) {
        super(source, container);
        this.connector = connector;
        this.timeSinceLastFetch = timeSinceLastFetch;
        this.listenerId = id;
        this.subscribe = subscribe;
    }

    public long getTimeSinceLastFetch() {
        return this.timeSinceLastFetch;
    }

    public String getListenerId() {
        return this.listenerId;
    }

    public String getSubscribe() {
        return this.subscribe;
    }

    @SuppressWarnings("unchecked")
    public <T> T getConnector(Class<T> type) {
        Assert.isInstanceOf(type, this.connector);
        return (T) this.connector;
    }
}
