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

/**
 * @author 橙子
 * @since 2020/11/26
 */
public class ContainerProperties {
    /**
     * <code>get.aim.memunit.count</code> maps {@link #getAimMemUnitCount}.
     */
    public static final String GET_AIM_MEM_UNIT_COUNT_CONFIG = "get.aim.memunit.count";
    /**
     * <code>get.timeout.ms</code> maps {@link #getTimeout}.
     */
    public static final String GET_TIMEOUT_MS_CONFIG = "get.timeout.ms";
    /**
     * Listener type. Only for auto-configuration passing error handler.
     */
    private Type type = Type.SINGLE;
    /**
     * Number of threads to run in the listener containers.
     */
    private Integer concurrency;
    private Duration idleBetweenFetches;
    /**
     * BatchSize to use when fetching the consumer.
     * The result entries size is as close to this value as possible.
     */
    private Integer getAimMemUnitCount = 1;
    /**
     * Timeout to use when fetching the consumer. Zero(default) means infinity.
     */
    private Duration getTimeout = Duration.ofMillis(3000);
    private Boolean startRollback = Boolean.TRUE;
    private Duration startTimout = Duration.ofSeconds(30);
    /**
     * Multiplier applied to "getTimeout" to determine if a consumer is
     * non-responsive.
     */
    private Float noGetThreshold = 3.0f;
    /**
     * Listener AckMode.
     */
    private AckMode ackMode = ContainerProperties.AckMode.SIMPLE;
    /**
     * Number of records between offset commits when ackMode is "COUNT" or
     * "COUNT_TIME".
     */
    private Integer ackCount = 1;
    /**
     * Time between offset commits when ackMode is "TIME" or "COUNT_TIME".
     */
    private Duration ackTime = Duration.ofMillis(5000);
    /**
     * Time between publishing idle consumer events (no data received).
     */
    private Duration maxIdleInterval;
    /**
     * Time between checks for valid consumers.
     */
    private Duration monitorInterval = Duration.ofSeconds(30);
    private Boolean logMetadataOnly = Boolean.TRUE;

    public Type getType() {
        return this.type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public Integer getConcurrency() {
        return this.concurrency;
    }

    public void setConcurrency(Integer concurrency) {
        this.concurrency = concurrency;
    }

    public Duration getIdleBetweenFetches() {
        return this.idleBetweenFetches;
    }

    public void setIdleBetweenFetches(Duration idleBetweenFetches) {
        this.idleBetweenFetches = idleBetweenFetches;
    }

    public Integer getGetAimMemUnitCount() {
        return this.getAimMemUnitCount;
    }

    public void setGetAimMemUnitCount(Integer getAimMemUnitCount) {
        this.getAimMemUnitCount = getAimMemUnitCount;
    }

    public Duration getGetTimeout() {
        return this.getTimeout;
    }

    public void setGetTimeout(Duration getTimeout) {
        this.getTimeout = getTimeout;
    }

    public Boolean isStartRollback() {
        return this.startRollback;
    }

    public void setStartRollback(Boolean startRollback) {
        this.startRollback = startRollback;
    }

    public Duration getStartTimout() {
        return this.startTimout;
    }

    public void setStartTimout(Duration startTimout) {
        this.startTimout = startTimout;
    }

    public Float getNoGetThreshold() {
        return this.noGetThreshold;
    }

    public void setNoGetThreshold(Float noGetThreshold) {
        this.noGetThreshold = noGetThreshold;
    }

    public AckMode getAckMode() {
        return this.ackMode;
    }

    public void setAckMode(AckMode ackMode) {
        this.ackMode = ackMode;
    }

    public Integer getAckCount() {
        return this.ackCount;
    }

    public void setAckCount(Integer ackCount) {
        this.ackCount = ackCount;
    }

    public Duration getAckTime() {
        return this.ackTime;
    }

    public void setAckTime(Duration ackTime) {
        this.ackTime = ackTime;
    }

    public Duration getMaxIdleInterval() {
        return this.maxIdleInterval;
    }

    public void setMaxIdleInterval(Duration maxIdleInterval) {
        this.maxIdleInterval = maxIdleInterval;
    }

    public Duration getMonitorInterval() {
        return this.monitorInterval;
    }

    public void setMonitorInterval(Duration monitorInterval) {
        this.monitorInterval = monitorInterval;
    }

    public Boolean isLogMetadataOnly() {
        return this.logMetadataOnly;
    }

    public void setLogMetadataOnly(Boolean logMetadataOnly) {
        this.logMetadataOnly = logMetadataOnly;
    }

    public enum Type {
        /**
         * Invokes the endpoint with one record at a time.
         */
        SINGLE,
        /**
         * Invokes the endpoint with a batch of records.
         */
        BATCH
    }

    public enum AckMode {
        /**
         * Commit before records is processed by the listener.
         */
        IMMEDIATELY,
        /**
         * Commit whatever has already been processed before(until) the next poll.
         */
        SIMPLE,
        /**
         * Commit pending updates after
         * {@link ContainerProperties#setAckTime(Duration) ackTime} has elapsed.
         */
        TIME,
        /**
         * Commit pending updates after
         * {@link ContainerProperties#setAckCount(Integer) ackCount} has been
         * exceeded.
         */
        COUNT,
        /**
         * Commit pending updates after
         * {@link ContainerProperties#setAckCount(Integer) ackCount} has been
         * exceeded or after {@link ContainerProperties#setAckTime(Duration)
         * ackTime} has elapsed.
         */
        COUNT_TIME,
        /**
         * User takes responsibility for acks using an
         * {@link org.springframework.canal.listener.EachAckMessageListener}
         * or {@link org.springframework.canal.listener.BatchAckMessageListener}.
         * The consumer immediately processes the commit.
         */
        MANUAL_IMMEDIATELY,
        /**
         * User takes responsibility for acks using an
         * {@link org.springframework.canal.listener.EachAckMessageListener}
         * or {@link org.springframework.canal.listener.BatchAckMessageListener}.
         * The consumer processes the commit before(until) the next poll.
         */
        MANUAL
    }
}
