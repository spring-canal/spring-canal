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

package org.springframework.canal.config;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author 橙子
 * @since 2020/11/26
 */
public class ConsumerProperties {
    private List<String> zkServers = new ArrayList<>();
    private List<String> hostname = new ArrayList<>(Collections.singletonList("localhost"));
    private List<Integer> port = new ArrayList<>(Collections.singletonList(11111));
    private String destination = "example";
    private String username = "canal";
    private String password = "canal";
    /**
     * Socket connect timeout.
     */
    private Duration soTimeout;
    /**
     * Network Read/Write transaction idle timeout.
     */
    private Duration idleTimeout;
    // Single
    private Boolean rollbackOnConnect;
    private Boolean rollbackOnDisConnect;
    private Boolean lazyParseEntry;
    // Single & Cluster
    /**
     * Must greater than zero.
     */
    private Integer retryTimes;
    private Duration retryInterval;

    public List<String> getZkServers() {
        return this.zkServers;
    }

    public void setZkServers(List<String> zkServers) {
        this.zkServers = zkServers;
    }

    public List<String> getHostname() {
        return this.hostname;
    }

    public void setHostname(List<String> hostname) {
        this.hostname = hostname;
    }

    public List<Integer> getPort() {
        return this.port;
    }

    public void setPort(List<Integer> port) {
        this.port = port;
    }

    public String getDestination() {
        return this.destination;
    }

    public void setDestination(String destination) {
        this.destination = destination;
    }

    public String getUsername() {
        return this.username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return this.password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public Duration getSoTimeout() {
        return this.soTimeout;
    }

    public void setSoTimeout(Duration soTimeout) {
        this.soTimeout = soTimeout;
    }

    public Duration getIdleTimeout() {
        return this.idleTimeout;
    }

    public void setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public Boolean getRollbackOnConnect() {
        return this.rollbackOnConnect;
    }

    public void setRollbackOnConnect(Boolean rollbackOnConnect) {
        this.rollbackOnConnect = rollbackOnConnect;
    }

    public Boolean getRollbackOnDisConnect() {
        return this.rollbackOnDisConnect;
    }

    public void setRollbackOnDisConnect(Boolean rollbackOnDisConnect) {
        this.rollbackOnDisConnect = rollbackOnDisConnect;
    }

    public Boolean getLazyParseEntry() {
        return this.lazyParseEntry;
    }

    public void setLazyParseEntry(Boolean lazyParseEntry) {
        this.lazyParseEntry = lazyParseEntry;
    }

    public Integer getRetryTimes() {
        return this.retryTimes;
    }

    public void setRetryTimes(Integer retryTimes) {
        this.retryTimes = retryTimes;
    }

    public Duration getRetryInterval() {
        return this.retryInterval;
    }

    public void setRetryInterval(Duration retryInterval) {
        this.retryInterval = retryInterval;
    }
}
