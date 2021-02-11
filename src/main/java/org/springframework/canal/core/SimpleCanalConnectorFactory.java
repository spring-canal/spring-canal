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
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.client.impl.ClusterCanalConnector;
import com.alibaba.otter.canal.client.impl.SimpleCanalConnector;

import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.canal.config.ConsumerProperties;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 橙子
 * @since 2020/11/17
 */
public class SimpleCanalConnectorFactory implements CanalConnectorFactory, FactoryBean<CanalConnector>, BeanFactoryAware {
    protected final ConsumerProperties properties;
    private BeanFactory beanFactory;

    public SimpleCanalConnectorFactory(ConsumerProperties properties) {
        this.properties = properties;
    }

    @Override
    public CanalConnector getObject() {
        CanalConnector canalConnector;
        if (!this.properties.getZkServers().isEmpty())
            canalConnector = CanalConnectors.newClusterConnector(String.join(",", this.properties.getZkServers()),
                    this.properties.getDestination(), this.properties.getUsername(), this.properties.getPassword());
        else {
            final List<String> hostname = this.properties.getHostname();
            final List<Integer> port = this.properties.getPort();
            hostname.addAll(Collections.nCopies(Math.max(hostname.size(), port.size()) - hostname.size(), "localhost"));
            port.addAll(Collections.nCopies(Math.max(hostname.size(), port.size()) - port.size(), 11111));
            final List<InetSocketAddress> addresses = new ArrayList<>(hostname.size());
            for (int i = 0; i < hostname.size(); i++)
                addresses.add(new InetSocketAddress(hostname.get(i), port.get(i)));
            if (addresses.size() > 1)
                canalConnector = CanalConnectors.newClusterConnector(addresses,
                        this.properties.getDestination(), this.properties.getUsername(), this.properties.getPassword());
            else
                canalConnector = CanalConnectors.newSingleConnector(addresses.get(0),
                        this.properties.getDestination(), this.properties.getUsername(), this.properties.getPassword());
        }
        SimpleCanalConnector simpleConnector = null;
        if (canalConnector instanceof SimpleCanalConnector)
            simpleConnector = ((SimpleCanalConnector) canalConnector);
        else if (canalConnector instanceof ClusterCanalConnector) {
            ClusterCanalConnector clusterConnector = ((ClusterCanalConnector) canalConnector);
            simpleConnector = clusterConnector.getCurrentConnector();
            configClusterConnector(clusterConnector);
        }
        if (simpleConnector != null)
            configSimpleConnector(simpleConnector);
        return canalConnector;
    }

    @Override
    public Class<?> getObjectType() {
        return CanalConnector.class;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public int getRetryTimes() {
        if (!this.properties.getZkServers().isEmpty() || this.properties.getHostname().size() > 1)
            return 1;
        return this.properties.getRetryTimes() == null ? 1 : this.properties.getRetryTimes();
    }

    @Override
    public int getRetryInterval() {
        return this.properties.getRetryInterval() == null ? 5000 : Math.toIntExact(this.properties.getRetryInterval().toMillis());
    }

    @Override
    public boolean isLazyParseEntry() {
        return this.properties.getLazyParseEntry() != null && this.properties.getLazyParseEntry();
    }

    @Override
    public void setBeanFactory(BeanFactory beanFactory) {
        this.beanFactory = beanFactory;
    }

    @Override
    public final CanalConnector getConnector() {
        return this.beanFactory == null ? getObject() : this.beanFactory.getBean(CanalConnector.class);
    }

    private void configClusterConnector(ClusterCanalConnector connector) {
        if (this.properties.getSoTimeout() != null)
            connector.setSoTimeout(Math.toIntExact(this.properties.getSoTimeout().toMillis()));
        if (this.properties.getIdleTimeout() != null)
            connector.setIdleTimeout(Math.toIntExact(this.properties.getIdleTimeout().toMillis()));
        if (this.properties.getRetryTimes() != null)
            connector.setRetryTimes(this.properties.getRetryTimes());
        if (this.properties.getRetryInterval() != null)
            connector.setRetryInterval(Math.toIntExact(this.properties.getRetryInterval().toMillis()));
    }

    private void configSimpleConnector(SimpleCanalConnector connector) {
        if (this.properties.getSoTimeout() != null)
            connector.setSoTimeout(Math.toIntExact(this.properties.getSoTimeout().toMillis()));
        if (this.properties.getIdleTimeout() != null)
            connector.setIdleTimeout(Math.toIntExact(this.properties.getIdleTimeout().toMillis()));
        if (this.properties.getRollbackOnConnect() != null)
            connector.setRollbackOnConnect(this.properties.getRollbackOnConnect());
        if (this.properties.getRollbackOnDisConnect() != null)
            connector.setRollbackOnDisConnect(this.properties.getRollbackOnDisConnect());
        if (this.properties.getLazyParseEntry() != null)
            connector.setLazyParseEntry(this.properties.getLazyParseEntry());
    }
}
