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

package org.springframework.canal.listener.container;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import org.apache.commons.logging.LogFactory;

import org.springframework.beans.BeanUtils;
import org.springframework.canal.config.ContainerProperties;
import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.canal.core.SmartCanalConnectorFactory;
import org.springframework.canal.event.ConsumerStartFailedEvent;
import org.springframework.canal.event.ConsumerStartSucceededEvent;
import org.springframework.canal.event.ConsumerStartingEvent;
import org.springframework.canal.event.ConsumerStoppedEvent;
import org.springframework.canal.event.ConsumerStoppingEvent;
import org.springframework.canal.event.ListenerContainerIdleEvent;
import org.springframework.canal.event.ListenerContainerNonResponsiveEvent;
import org.springframework.canal.listener.BatchMessageListener;
import org.springframework.canal.listener.ListenerUtils;
import org.springframework.canal.listener.MessageListener;
import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;
import org.springframework.canal.support.CanalMessageUtils;
import org.springframework.canal.transaction.CanalAwareTransactionManager;
import org.springframework.canal.transaction.CanalResourceHolder;
import org.springframework.canal.transaction.CanalResourceHolderUtils;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.log.LogAccessor;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.scheduling.SchedulingAwareRunnable;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.Assert;

import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @param <T> the entity type; can be {@link com.alibaba.otter.canal.protocol.CanalEntry CanalEntry.Entry} or {@link com.google.protobuf.ByteString ByteString}
 * @author 橙子
 * @since 2020/11/15
 */
public class DefaultMessageListenerContainer<T> extends AbstractMessageListenerContainer {
    private final MessageListenerContainer realContainer;
    private final Queue<Future<?>> tasks;

    public DefaultMessageListenerContainer(CanalConnectorFactory connectorFactory, ContainerProperties containerProperties, MessageListener<?> listener, Collection<String> subscribes, Properties supersedeProperties) {
        this(null, connectorFactory, containerProperties, listener, subscribes, supersedeProperties);
    }

    public DefaultMessageListenerContainer(MessageListenerContainer parentContainer, CanalConnectorFactory connectorFactory, ContainerProperties containerProperties, MessageListener<?> listener, Collection<String> subscribes, Properties supersedeProperties) {
        super(connectorFactory, containerProperties, listener, subscribes, supersedeProperties);
        this.realContainer = parentContainer == null ? this : parentContainer;
        this.tasks = new ConcurrentLinkedQueue<>();
    }

    @Override
    public String getBeanName() {
        final String beanName = super.getBeanName();
        return beanName == null ? "" : beanName + "-";
    }

    @Override
    public void start() {
        this.running = true;
        if (ContainerProperties.AckMode.COUNT_TIME.equals(this.containerProperties.getAckMode()) || ContainerProperties.AckMode.COUNT.equals(this.containerProperties.getAckMode()))
            Assert.state(containerProperties.getAckCount() > 0, "'ackCount' must be > 0");
        if (ContainerProperties.AckMode.COUNT_TIME.equals(this.containerProperties.getAckMode()) || ContainerProperties.AckMode.TIME.equals(this.containerProperties.getAckMode()))
            Assert.state(!containerProperties.getAckTime().isZero() && !containerProperties.getAckTime().isNegative(), "'ackTime' must be > 0");
        final CountDownLatch startLatch = new CountDownLatch(1);
        this.tasks.add(this.listenerTaskExecutor.updateAndGet(taskExecutor -> taskExecutor == null ? new SimpleAsyncTaskExecutor(getBeanName()) : taskExecutor)
                .submit(new MessageListenerTask(this.listener, ListenerUtils.determineListenerParameterType(this.listener), startLatch)));
        try {
            if (!startLatch.await(this.containerProperties.getStartTimout().toMillis(), TimeUnit.MILLISECONDS)) {
                this.logger.error("Consumer thread failed to start - does the configured task executor have enough " +
                        "threads to support all containers and concurrency?");
                publishConsumerStartFailedEvent();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void stop(boolean wait) {
        this.running = false;
        if (wait) {
            Future<?> current;
            while ((current = this.tasks.poll()) != null)
                try {
                    current.get();
                } catch (ExecutionException ignored) {
                    // Do nothing because of wait all tasks.
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
        }
    }

    private void publishConsumerStartingEvent(CanalConnector connector) {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ConsumerStartingEvent(connector, this, this.realContainer));
    }

    private void publishConsumerStartSucceededEvent(CanalConnector connector) {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ConsumerStartSucceededEvent(connector, this, this.realContainer));
    }

    private void publishConsumerStartFailedEvent() {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ConsumerStartFailedEvent(this, this.realContainer));
    }

    private void publishConsumerStoppingEvent(CanalConnector connector) {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ConsumerStoppingEvent(connector, this, this.realContainer));
    }

    private void publishConsumerStoppedEvent(CanalConnector connector) {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ConsumerStoppedEvent(connector, this, this.realContainer));
    }

    private void publishIdleContainerEvent(long idleTime, CanalConnector connector) {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ListenerContainerIdleEvent(idleTime, connector, this, this.realContainer));
    }

    private void publishNonResponsiveContainerEvent(long timeSinceLastFetch, CanalConnector connector) {
        final ApplicationEventPublisher publisher = getApplicationEventPublisher();
        if (publisher != null)
            publisher.publishEvent(new ListenerContainerNonResponsiveEvent(timeSinceLastFetch, getBeanName(),
                    String.join(",", this.subscribes), connector, this, this.realContainer));
    }

    private final class MessageListenerTask implements SchedulingAwareRunnable {
        protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
        private final CanalConnectorFactory connectorFactory;
        /**
         * Generic parameter type can be T({@link com.alibaba.otter.canal.protocol.CanalEntry.Entry CanalEntry.Entry} or {@link com.google.protobuf.ByteString ByteString})
         * or Collection&lt;T>({@link Collection<com.alibaba.otter.canal.protocol.CanalEntry.Entry> Collection&lt;CanalEntry.Entry>} or {@link Collection<com.google.protobuf.ByteString> Collection&lt;ByteString>}).
         */
        private final MessageListener<?> listener;
        private final boolean isBatchListener;
        private final ListenerUtils.ParameterType parameterType;
        private final ContainerProperties containerProperties;
        private final boolean manualAckMode;
        private final TaskScheduler monitorScheduler;
        private final TransactionTemplate transactionTemplate;
        private final CanalConnector connector;
        /**
         * Generic parameter type can be T({@link com.alibaba.otter.canal.protocol.CanalEntry.Entry CanalEntry.Entry} or {@link com.google.protobuf.ByteString ByteString})
         * or {@link Message}.
         */
        private final CanalListenerContainerErrorHandler<?> errorHandler;
        private final long getTimeout;
        private final int getAimMemUnitCount;
        private final BlockingQueue<Message> ackQueue;
        /**
         * Transaction support wait for ack queue.
         */
        private final BlockingQueue<Message> waitQueue;
        private final AtomicReference<Thread> consumerThread;
        private final ScheduledFuture<?> monitorTask;
        private final CountDownLatch startLatch;
        private volatile long lastFetchingTime;
        private boolean defaultMonitorScheduler;
        private int ackCount;
        private long ackTime;
        private long lastFetchedNonEmptyTime;
        private long lastIdleTime;
        private boolean fatalError;
        private long negativelyAckSleep;

        private MessageListenerTask(MessageListener<?> listener, ListenerUtils.ParameterType parameterType, CountDownLatch startLatch) {
            final String subscribe = String.join(",", DefaultMessageListenerContainer.this.subscribes);
            this.connectorFactory = DefaultMessageListenerContainer.this.getTransactionManager() instanceof CanalAwareTransactionManager ?
                    ((CanalAwareTransactionManager) DefaultMessageListenerContainer.this.getTransactionManager()).getConnectorFactory()
                    : DefaultMessageListenerContainer.this.connectorFactory;
            if (this.connectorFactory instanceof SmartCanalConnectorFactory)
                ((SmartCanalConnectorFactory) this.connectorFactory).setSubscribe(subscribe);
            this.listener = listener;
            this.isBatchListener = this.listener instanceof BatchMessageListener;
            if (this.isBatchListener)
                ((BatchMessageListener<?>) this.listener).setRawEntry(this.connectorFactory.isLazyParseEntry());
            this.parameterType = parameterType;
            this.containerProperties = DefaultMessageListenerContainer.this.containerProperties;
            this.manualAckMode = ContainerProperties.AckMode.MANUAL.equals(this.containerProperties.getAckMode())
                    || ContainerProperties.AckMode.MANUAL_IMMEDIATELY.equals(this.containerProperties.getAckMode());
            this.monitorScheduler = getMonitorScheduler();
            if (DefaultMessageListenerContainer.this.getTransactionManager() == null) {
                this.transactionTemplate = null;
                this.connector = this.connectorFactory.getConnector();
                ListenerUtils.subscribeFilters(this.connector,
                        subscribe,
                        this.connectorFactory.getRetryTimes(),
                        this.connectorFactory.getRetryInterval(),
                        this.containerProperties.isStartRollback());
                if (DefaultMessageListenerContainer.this.getErrorHandler() == null) {
                    if (this.isBatchListener)
                        this.errorHandler = new LoggingCanalListenerContainerBatchErrorHandler(this.containerProperties.isLogMetadataOnly());
                    else
                        this.errorHandler = new LoggingCanalListenerContainerEachErrorHandler<>(this.containerProperties.isLogMetadataOnly());
                } else
                    this.errorHandler = DefaultMessageListenerContainer.this.getErrorHandler();
            } else {
                this.transactionTemplate = new TransactionTemplate(DefaultMessageListenerContainer.this.getTransactionManager());
                if (DefaultMessageListenerContainer.this.getTransactionDefinition() != null)
                    BeanUtils.copyProperties(DefaultMessageListenerContainer.this.getTransactionDefinition(), this.transactionTemplate);
                this.connector = this.transactionTemplate.execute(status -> {
                    final CanalResourceHolder canalResourceHolder = CanalResourceHolderUtils.getTransactionalResourceHolder(this.connectorFactory);
                    canalResourceHolder.requested();
                    canalResourceHolder.connect(subscribe,
                            MessageListenerTask.this.connectorFactory.getRetryTimes(),
                            MessageListenerTask.this.connectorFactory.getRetryInterval(),
                            MessageListenerTask.this.containerProperties.isStartRollback());
                    return canalResourceHolder.getCanalConnector();
                });
                this.errorHandler = DefaultMessageListenerContainer.this.getErrorHandler();
            }
            this.getTimeout = ListenerUtils.parseLong(DefaultMessageListenerContainer.this.supersedeProperties.getOrDefault(
                    ContainerProperties.GET_TIMEOUT_MS_CONFIG,
                    this.containerProperties.getGetTimeout().toMillis()));
            this.getAimMemUnitCount = ListenerUtils.parseInteger(DefaultMessageListenerContainer.this.supersedeProperties.getOrDefault(
                    ContainerProperties.GET_AIM_MEM_UNIT_COUNT_CONFIG,
                    this.containerProperties.getGetAimMemUnitCount()));
            this.ackQueue = new LinkedBlockingQueue<>();
            this.waitQueue = new LinkedBlockingQueue<>();
            this.consumerThread = new AtomicReference<>();
            this.monitorTask = this.monitorScheduler.scheduleAtFixedRate(this::doMonitor, this.containerProperties.getMonitorInterval());
            this.startLatch = startLatch;
        }

        @Override
        public boolean isLongLived() {
            return true;
        }

        @Override
        public void run() {
            if (this.startLatch != null)
                this.startLatch.countDown();
            DefaultMessageListenerContainer.this.publishConsumerStartingEvent(this.connector);
            this.consumerThread.compareAndSet(this.consumerThread.get(), Thread.currentThread());
            this.ackCount = 0;
            this.lastIdleTime = System.currentTimeMillis();
            this.lastFetchedNonEmptyTime = System.currentTimeMillis();
            this.ackTime = System.currentTimeMillis();
            this.fatalError = false;
            this.negativelyAckSleep = -1;
            DefaultMessageListenerContainer.this.publishConsumerStartSucceededEvent(this.connector);
            while (DefaultMessageListenerContainer.this.isRunning())
                try {
                    fetchAndInvoke();
                } catch (CanalClientException e) {
                    if (this.getTimeout != 0 || !(e.getCause() instanceof SocketTimeoutException)) {
                        this.fatalError = true;
                        break;
                    }
                } catch (Exception e) {
                    handleException(e);
                } catch (Error e) { // NOSONAR
                    this.logger.error(e, "Stopping container due to an Error");
                    close();
                    throw e;
                }
            close();
        }

        /**
         * Start schedule invoked when instance this object.
         */
        protected void doMonitor() {
            if (this.containerProperties.getNoGetThreshold() != null && this.getTimeout != 0) {
                long timeSinceLastFetch = System.currentTimeMillis() - this.lastFetchingTime;
                if (timeSinceLastFetch / (float) this.getTimeout > this.containerProperties.getNoGetThreshold())
                    DefaultMessageListenerContainer.this.publishNonResponsiveContainerEvent(timeSinceLastFetch, this.connector);
            }
            if (this.consumerThread.get() != null) {
                Thread.currentThread().setName(this.consumerThread.get().getName() + "-monitor");
                if (!this.connector.checkValid())
                    DefaultMessageListenerContainer.this.stop();
            }
        }

        private void fetchAndInvoke() {
            // Ack the last batch
            doAck();
            doIdle();
            this.lastFetchingTime = System.currentTimeMillis();
            if (!DefaultMessageListenerContainer.this.isRunning())
                return;
            Message message = doFetch();
            if (!CanalMessageUtils.isEmpty(message))
                this.logger.debug(() -> "Received: " + CanalMessageUtils.getSize(message) + " records");
            checkIdle(message);
            if (!CanalMessageUtils.isEmpty(message)) {
                this.logger.trace(() -> CanalMessageUtils.toString(message, this.containerProperties.isLogMetadataOnly()));
                invokeListener(message);
            }
        }

        private void doIdle() {
            final Duration idleBetweenFetches = this.containerProperties.getIdleBetweenFetches();
            if (idleBetweenFetches != null && !idleBetweenFetches.isZero() && !idleBetweenFetches.isNegative())
                try {
                    Thread.sleep(idleBetweenFetches.toMillis());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("Consumer Thread [" + this + "] has been interrupted", e);
                }
        }

        private void checkIdle(Message message) {
            final Duration maxIdleInterval = this.containerProperties.getMaxIdleInterval();
            if (maxIdleInterval == null)
                return;
            final long now = System.currentTimeMillis();
            if (!CanalMessageUtils.isEmpty(message))
                this.lastFetchedNonEmptyTime = now;
            else if (now > Math.max(this.lastFetchedNonEmptyTime, this.lastIdleTime) + maxIdleInterval.toMillis()) {
                this.lastIdleTime = now;
                DefaultMessageListenerContainer.this.publishIdleContainerEvent(now - this.lastFetchedNonEmptyTime, this.connector);
                // todo: Invoke SeekAwareListener for set offset
            }
        }

        private Message doFetch() {
            final int batchSize = ContainerProperties.AckMode.TIME.equals(this.containerProperties.getAckMode())
                    || ContainerProperties.AckMode.COUNT.equals(this.containerProperties.getAckMode())
                    || ContainerProperties.AckMode.COUNT_TIME.equals(this.containerProperties.getAckMode()) ?
                    1 : this.getAimMemUnitCount;
            return this.connector.getWithoutAck(batchSize, this.getTimeout, TimeUnit.MILLISECONDS);
        }

        private void invokeListener(Message records) {
            if (this.isBatchListener)
                invokeBatchListener(records, null);
            else
                invokeEachListener(records);
        }

        @SuppressWarnings("ThrowableNotThrown")
        private void invokeEachListener(Message records) {
            Iterator<T> iterator = CanalMessageUtils.<T>getEntries(records).iterator();
            while (iterator.hasNext()) {
                final T originEntry = iterator.next();
                if (originEntry == null)
                    continue;
                T entry = originEntry;
                // entry = checkIntercept(entry)
                // if (entry == null) {
                //     this.logger.debug(() -> "RecordInterceptor returned null, skipping: " + originEntry)
                //     continue
                // }
                this.logger.trace(() -> "Processing " + CanalMessageUtils.toString(entry, this.containerProperties.isLogMetadataOnly()));
                if (this.transactionTemplate == null)
                    doInvokeEachListener(records, iterator, entry);
                else
                    try {
                        this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                            @Override
                            protected void doInTransactionWithoutResult(TransactionStatus status) {
                                final RuntimeException runtimeException = doInvokeEachListener(records, iterator, entry);
                                if (runtimeException != null) {
                                    MessageListenerTask.this.waitQueue.clear();
                                    MessageListenerTask.this.handleAck();
                                    throw runtimeException;
                                }
                            }
                        });
                    } catch (RuntimeException e) {
                        this.logger.error(e, "Transaction rolled back");
//                        recordAfterRollback(iterator, entry, e)
                    }
                if (this.negativelyAckSleep >= 0) {
                    doAck();
                    try {
                        Thread.sleep(this.negativelyAckSleep);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    this.negativelyAckSleep = -1;
                    break;
                }
            }
        }

        @SuppressWarnings("ThrowableNotThrown")
        private void invokeBatchListener(Message records, CanalEntries<T> recordCollection) {
            if (this.transactionTemplate == null)
                doInvokeBatchListener(records, recordCollection);
            else
                try {
                    this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                        @Override
                        protected void doInTransactionWithoutResult(TransactionStatus status) {
                            final RuntimeException exception = doInvokeBatchListener(records, recordCollection);
                            if (exception != null) {
                                MessageListenerTask.this.waitQueue.clear();
                                MessageListenerTask.this.handleAck();
                                throw exception;
                            }
                        }
                    });
                } catch (RuntimeException e) {
                    this.logger.error(e, "Transaction rolled back");
//                    batchAfterRollback(records, recordCollection, e)
                }
            if (this.negativelyAckSleep >= 0) {
                doAck();
                try {
                    Thread.sleep(this.negativelyAckSleep);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                this.negativelyAckSleep = -1;
            }
        }

        private RuntimeException doInvokeEachListener(Message record, Iterator<T> iterator, T entry) {
            try {
                try {
                    onMessage(record, entry);
                    if (this.negativelyAckSleep < 0 && !ContainerProperties.AckMode.MANUAL_IMMEDIATELY.equals(this.containerProperties.getAckMode()))
                        ack(record, this.manualAckMode);
                } catch (RuntimeException e) {
                    if (this.errorHandler == null)
                        throw e;
                    try {
                        handleException(entry, iterator, e);
                        if (this.errorHandler.isAckAfterExceptionHandle()) {
                            this.negativelyAckSleep = -1;
                            ack(record, false);
                        }
                    } catch (RuntimeException runtimeException) {
                        this.logger.error(runtimeException, "Error handler threw an exception");
                        return runtimeException;
                    } catch (Error error) { // NOSONAR
                        this.logger.error(error, "Error handler threw an error");
                        throw error;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }

        private RuntimeException doInvokeBatchListener(Message records, CanalEntries<T> recordCollection) {
            try {
                try {
                    if (recordCollection == null)
                        recordCollection = CanalMessageUtils.getEntries(records);
                    if (((BatchMessageListener<?>) this.listener).wantsOriginMessage(recordCollection))
                        ((BatchMessageListener<?>) this.listener).onMessage(records, this.manualAckMode ? new ConsumerAcknowledgment(records) : null, this.connector);
                    else
                        onMessage(records, recordCollection);
                    if (this.negativelyAckSleep < 0)
                        ack(records, this.manualAckMode);
                } catch (RuntimeException e) {
                    if (this.errorHandler == null)
                        throw e;
                    try {
                        handleException(records, recordCollection, e);
                        if (this.errorHandler.isAckAfterExceptionHandle()) {
                            this.negativelyAckSleep = -1;
                            ack(records, false);
                        }
                    } catch (RuntimeException runtimeException) {
                        this.logger.error(runtimeException, "Error handler threw an exception");
                        return runtimeException;
                    } catch (Error error) { // NOSONAR
                        this.logger.error(error, "Error handler threw an error");
                        throw error;
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return null;
        }

        private void onMessage(Message records, Object data) {
            switch (this.parameterType) {
                case ACK_CONNECTOR_AWARE:
                    ((MessageListener<Object>) this.listener).onMessage(data, this.manualAckMode ? new ConsumerAcknowledgment(records) : null, this.connector);
                    break;
                case ACK:
                    ((MessageListener<Object>) this.listener).onMessage(data, this.manualAckMode ? new ConsumerAcknowledgment(records) : null);
                    break;
                case CONNECTOR_AWARE:
                    ((MessageListener<Object>) this.listener).onMessage(data, this.connector);
                    break;
                case SIMPLE:
                    ((MessageListener<Object>) this.listener).onMessage(data);
                    break;
            }
        }

        private void ack(Message message, boolean isAnyManualAck) throws InterruptedException {
            if (ContainerProperties.AckMode.IMMEDIATELY.equals(this.containerProperties.getAckMode())) {
                if (!this.waitQueue.contains(message))
                    return;
                this.logger.info(() -> "Immediately Committing: " + message.getId());
                this.connector.ack(message.getId());
                this.waitQueue.put(message);
            } else if (!isAnyManualAck)
                this.waitQueue.put(message);
        }

        private void doAck() {
            this.ackQueue.addAll(this.waitQueue);
            this.waitQueue.clear();
            switch (this.containerProperties.getAckMode()) {
                case COUNT:
                case COUNT_TIME:
                    if (this.isBatchListener)
                        ++this.ackCount;
                    else
                        this.ackCount += CanalMessageUtils.getSize(this.ackQueue.peek());
                    if (this.ackCount > this.containerProperties.getAckCount()) {
                        this.logger.debug(() -> "Committing in " + this.containerProperties.getAckMode().name() + " because count "
                                + this.ackCount
                                + " exceeds configured limit of " + this.containerProperties.getAckCount());
                        handleAck();
                        this.ackCount = 0;
                        break;
                    }
                case TIME:
                    final long now = System.currentTimeMillis();
                    if (now - this.ackTime > this.containerProperties.getAckTime().toMillis()) {
                        this.logger.debug(() -> "Committing in AckMode." + this.containerProperties.getAckMode().name() +
                                " because time elapsed exceeds configured limit of " +
                                this.containerProperties.getAckTime());
                        handleAck();
                        this.ackTime = now;
                    }
                default:
                    handleAck();
                    break;
            }
        }

        private void handleAck() {
            final Message last = this.ackQueue.poll();
            if (last != null) {
                this.logger.info(() -> "Committing: " + last.getId());
                this.connector.ack(last.getId());
                this.ackQueue.clear();
            }
        }

        private void close() {
            DefaultMessageListenerContainer.this.publishConsumerStoppingEvent(this.connector);
            if (this.fatalError) {
                this.logger.error("Fatal consumer exception; stopping container");
                // Immediately stop
                DefaultMessageListenerContainer.this.stop();
            } else {
                // Ack the last batch
                if (this.transactionTemplate == null) {
                    handleAck();
                    // Canal Bug: When simple connectors subscribe the same filters, client identities are the same too (field 'clientId' is const),
                    // server will confuse other clients did not subscribe after any unsubscribe.
                    /*
                      try {
                           this.connector.unsubscribe()
                      } catch (CanalClientException ignored) {
                          // Do nothing because of disconnect finally.
                      }
                    */
                }
            }
            this.monitorTask.cancel(true);
            if (this.defaultMonitorScheduler)
                ((ThreadPoolTaskScheduler) this.monitorScheduler).destroy();
            if (this.transactionTemplate == null)
                this.connector.disconnect();
            else
                this.transactionTemplate.execute(new TransactionCallbackWithoutResult() {
                    @Override
                    protected void doInTransactionWithoutResult(TransactionStatus status) {
                        CanalResourceHolderUtils.getTransactionalResourceHolder(MessageListenerTask.this.connectorFactory).released();
                    }
                });
//            getAfterRollbackProcessor().clearThreadState()
            if (this.errorHandler != null)
                this.errorHandler.clearThreadState();
            this.logger.info(() -> "Consumer stopped");
            DefaultMessageListenerContainer.this.publishConsumerStoppedEvent(this.connector);
        }

        private void handleException(Exception e) {
            if (this.errorHandler instanceof CanalListenerContainerEachErrorHandler)
                handleException((T) null, null, e);
            else if (this.errorHandler instanceof CanalListenerContainerBatchErrorHandler)
                handleException(new Message(-1, false, Collections.emptyList()), null, e);
        }

        private void handleException(Message elements, CanalEntries<T> recordCollection, Exception e) {
            ((CanalListenerContainerBatchErrorHandler) this.errorHandler).handle(e, elements, this.connector,
                    DefaultMessageListenerContainer.this.realContainer, () -> invokeBatchListener(elements, recordCollection));
        }

        private void handleException(T element, Iterator<T> iterator, Exception e) {
            // todo: CanalListenerContainerRemainingErrorHandler for remaining consume
            ((CanalListenerContainerEachErrorHandler<T>) this.errorHandler).handle(e, element, this.connector,
                    DefaultMessageListenerContainer.this.realContainer);
        }

        private TaskScheduler getMonitorScheduler() {
            final TaskScheduler scheduler = DefaultMessageListenerContainer.this.getMonitorScheduler();
            this.defaultMonitorScheduler = scheduler == null;
            if (scheduler != null)
                return scheduler;
            final ThreadPoolTaskScheduler poolTaskScheduler = new ThreadPoolTaskScheduler();
            poolTaskScheduler.initialize();
            return poolTaskScheduler;
        }

        private final class ConsumerAcknowledgment implements Acknowledgment {
            private final Message message;

            ConsumerAcknowledgment(Message message) {
                this.message = message;
            }

            @Override
            public void ack() {
                if (Thread.currentThread().equals(MessageListenerTask.this.consumerThread.get())
                        && ContainerProperties.AckMode.MANUAL_IMMEDIATELY.equals(MessageListenerTask.this.containerProperties.getAckMode())) {
                    // Sync invoking from ConsumerAcknowledgment's method 'ack'
                    if (!MessageListenerTask.this.waitQueue.contains(this.message))
                        return;
                    MessageListenerTask.this.logger.info(() -> "Manual Immediately Committing: " + this.message.getId());
                    MessageListenerTask.this.connector.ack(this.message.getId());
                }
                try {
                    MessageListenerTask.this.waitQueue.put(this.message);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new CanalException("Interrupted while storing ack", e);
                }
            }

            @Override
            public void nack(long sleep) {
                Assert.state(Thread.currentThread().equals(MessageListenerTask.this.consumerThread.get()),
                        "negativelyAck(sleep) can only be called on the consumer thread");
                Assert.isTrue(sleep >= 0, "sleep cannot be negative");
                MessageListenerTask.this.negativelyAckSleep = sleep;
            }
        }
    }
}
