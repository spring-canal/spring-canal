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

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import net.bytebuddy.utility.RandomString;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.canal.annotation.CanalBefore;
import org.springframework.canal.annotation.CanalMessageHandlerMethodFactoryTest;
import org.springframework.canal.annotation.listener.CanalListener;
import org.springframework.canal.config.ConsumerProperties;
import org.springframework.canal.config.ContainerProperties;
import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.canal.core.SimpleCanalConnectorFactory;
import org.springframework.canal.listener.adapter.BatchMessageListenerAdapter;
import org.springframework.canal.listener.adapter.EachMessageListenerAdapter;
import org.springframework.canal.model.TestBean;
import org.springframework.canal.support.CanalEntries;
import org.springframework.canal.support.converter.message.CanalJpaMessageConverter;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;
import org.springframework.canal.transaction.CanalTransactionManager;
import org.springframework.canal.transaction.ChainedCanalTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.annotation.support.MessageHandlerMethodFactory;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.TransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author 橙子
 * @since 2021/1/2
 */
@SpringJUnitConfig(classes = DefaultMessageListenerContainerTest.AppConfig.class)
class DefaultMessageListenerContainerTest {
    @Autowired
    private EntityManagerFactory entityManagerFactory;
    @Autowired
    private MessageHandlerMethodFactory messageHandlerMethodFactory;
    @Autowired
    private ListenerConfig listenerConfig;
    @Autowired
    private CanalConnectorFactory canalConnectorFactory;
    @Qualifier("lazyConnectorFactory")
    @Autowired
    private CanalConnectorFactory lazyCanalConnectorFactory;
    @Autowired
    private ContainerProperties containerProperties;
    private AbstractMessageListenerContainer canalEntrySingleMethodEachListenerContainer;
    private AbstractMessageListenerContainer byteStringSingleMethodEachListenerContainer;
    private AbstractMessageListenerContainer canalEntrySingleMethodBatchListenerContainer;
    private AbstractMessageListenerContainer byteStringSingleMethodBatchListenerContainer;

    @BeforeEach
    void setUp() {
        prepareData();
        final Map<Method, CanalListener> eachMethods = MethodIntrospector.selectMethods(this.listenerConfig.getClass(), (MethodIntrospector.MetadataLookup<CanalListener>) method ->
                method.getName().contains("each") ? AnnotatedElementUtils.findMergedAnnotation(method, CanalListener.class) : null);
        final Map<Method, CanalListener> batchMethods = MethodIntrospector.selectMethods(this.listenerConfig.getClass(), (MethodIntrospector.MetadataLookup<CanalListener>) method ->
                method.getName().contains("batch") ? AnnotatedElementUtils.findMergedAnnotation(method, CanalListener.class) : null);
        final EachMessageListenerAdapter<CanalEntry.Entry> singleMethodEachListenerAdapter = new EachMessageListenerAdapter<>(this.listenerConfig,
                null,
                eachMethods.keySet().stream().map(method -> this.messageHandlerMethodFactory.createInvocableHandlerMethod(this.listenerConfig, method)).collect(Collectors.toList()),
                null);
        final BatchMessageListenerAdapter<CanalEntry.Entry> singleMethodBatchListenerAdapter = new BatchMessageListenerAdapter<>(this.listenerConfig,
                null,
                batchMethods.keySet().stream().map(method -> this.messageHandlerMethodFactory.createInvocableHandlerMethod(this.listenerConfig, method)).collect(Collectors.toList()),
                null);
        this.canalEntrySingleMethodEachListenerContainer = new DefaultMessageListenerContainer<CanalEntry.Entry>(this.canalConnectorFactory,
                this.containerProperties,
                singleMethodEachListenerAdapter,
                eachMethods.values().stream().map(CanalListener::subscribes).flatMap(Arrays::stream).collect(Collectors.toList()),
                new Properties());
        this.byteStringSingleMethodEachListenerContainer = new DefaultMessageListenerContainer<ByteString>(this.lazyCanalConnectorFactory,
                this.containerProperties,
                singleMethodEachListenerAdapter,
                eachMethods.values().stream().map(CanalListener::subscribes).flatMap(Arrays::stream).collect(Collectors.toList()),
                new Properties());
        this.canalEntrySingleMethodBatchListenerContainer = new DefaultMessageListenerContainer<CanalEntry.Entry>(this.canalConnectorFactory,
                this.containerProperties,
                singleMethodBatchListenerAdapter,
                batchMethods.values().stream().map(CanalListener::subscribes).flatMap(Arrays::stream).collect(Collectors.toList()),
                new Properties());
        this.byteStringSingleMethodBatchListenerContainer = new DefaultMessageListenerContainer<ByteString>(this.lazyCanalConnectorFactory,
                this.containerProperties,
                singleMethodBatchListenerAdapter,
                batchMethods.values().stream().map(CanalListener::subscribes).flatMap(Arrays::stream).collect(Collectors.toList()),
                new Properties());
        this.canalEntrySingleMethodEachListenerContainer.setBeanName("EntrySingleMethodEachListener");
        this.byteStringSingleMethodEachListenerContainer.setBeanName("RawSingleMethodEachListener");
        this.canalEntrySingleMethodBatchListenerContainer.setBeanName("EntrySingleMethodBatchListener");
        this.byteStringSingleMethodBatchListenerContainer.setBeanName("RawSingleMethodBatchListener");
    }

    @Test
    void canalEntrySingleMethodEachListener() throws InterruptedException {
        this.canalEntrySingleMethodEachListenerContainer.start();
        assertNotNull(this.canalEntrySingleMethodEachListenerContainer.getListenerTaskExecutor());
        synchronized (this) {
            wait(5000);
        }
        assertTrue(this.canalEntrySingleMethodEachListenerContainer.isRunning());
        this.canalEntrySingleMethodEachListenerContainer.stop();
    }

    @Test
    void byteStringSingleMethodEachListener() throws InterruptedException {
        this.byteStringSingleMethodEachListenerContainer.start();
        assertNotNull(this.byteStringSingleMethodEachListenerContainer.getListenerTaskExecutor());
        synchronized (this) {
            wait(5000);
        }
        assertTrue(this.byteStringSingleMethodEachListenerContainer.isRunning());
        this.byteStringSingleMethodEachListenerContainer.stop();
    }

    @Test
    void canalEntrySingleMethodBatchListener() throws InterruptedException {
        this.canalEntrySingleMethodBatchListenerContainer.start();
        assertNotNull(this.canalEntrySingleMethodBatchListenerContainer.getListenerTaskExecutor());
        synchronized (this) {
            wait(5000);
        }
        assertTrue(this.canalEntrySingleMethodBatchListenerContainer.isRunning());
        this.canalEntrySingleMethodBatchListenerContainer.stop();
    }

    @Test
    void byteStringSingleMethodBatchListener() throws InterruptedException {
        this.byteStringSingleMethodBatchListenerContainer.start();
        assertNotNull(this.byteStringSingleMethodBatchListenerContainer.getListenerTaskExecutor());
        synchronized (this) {
            wait(5000);
        }
        assertTrue(this.byteStringSingleMethodBatchListenerContainer.isRunning());
        this.byteStringSingleMethodBatchListenerContainer.stop();
    }

    @AfterEach
    void tearDown() {
        this.canalEntrySingleMethodEachListenerContainer.stop();
        this.byteStringSingleMethodEachListenerContainer.stop();
        this.canalEntrySingleMethodBatchListenerContainer.stop();
        this.byteStringSingleMethodBatchListenerContainer.stop();
    }

    private void prepareData() {
        final Session session = this.entityManagerFactory.createEntityManager().unwrap(Session.class);
        final Transaction transaction = session.beginTransaction();
        final TestBean testBean = new TestBean(1, "Hello");
        session.save(testBean);
        session.remove(testBean);
        session.saveOrUpdate(new TestBean(2, "Spring"));
        session.saveOrUpdate(new TestBean(3, RandomString.make()));
        transaction.commit();
        session.close();
    }

    @Configuration
    @Import(ListenerConfig.class)
    static class AppConfig {
        @Bean
        public ContainerProperties containerProperties() {
            final ContainerProperties containerProperties = new ContainerProperties();
            return containerProperties;
        }

        @Primary
        @Bean
        public CanalConnectorFactory connectorFactory() {
            final ConsumerProperties properties = new ConsumerProperties();
            configConsumerProperties(properties);
            properties.setLazyParseEntry(false);
            return new SimpleCanalConnectorFactory(properties);
        }

        @Bean("lazyConnectorFactory")
        public CanalConnectorFactory lazyConnectorFactory() {
            final ConsumerProperties properties = new ConsumerProperties();
            configConsumerProperties(properties);
            properties.setLazyParseEntry(true);
            return new SimpleCanalConnectorFactory(properties);
        }

        @Bean
        public MessageHandlerMethodFactory messageHandlerMethodFactory(EntityManagerFactory entityManagerFactory) {
            final DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = CanalMessageHandlerMethodFactoryTest.getInstance(
                    new CanalJpaMessageConverter(entityManagerFactory, true));
            return messageHandlerMethodFactory;
        }

        @Bean
        public EntityManagerFactory entityManagerFactory() {
            return Persistence.createEntityManagerFactory("org.springframework.canal.listener");
        }

        @Bean
        public TransactionManager transactionManager(EntityManagerFactory entityManagerFactory,
                                                     CanalConnectorFactory canalConnectorFactory) {
            return new ChainedCanalTransactionManager(
                    new JpaTransactionManager(entityManagerFactory),
                    new CanalTransactionManager(canalConnectorFactory));
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

    @Configuration
    static class ListenerConfig {
        private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ListenerConfig.class));
        @Autowired
        private EntityManagerFactory entityManagerFactory;
        @PersistenceContext
        private EntityManager entityManager;

        @Transactional
        @CanalListener(subscribes = "test\\.tb_canal_listener_test")
        public void eachListen(@Header(CanalMessageHeaderAccessor.SCHEMA_NAME) String schemaName,
                               @Header(CanalMessageHeaderAccessor.TABLE_NAME) String tableName,
                               @Payload Collection<TestBean> payloads,
                               Message<Collection<TestBean>> payloadsMessage, CanalEntry.Entry originRecord) {
            LOGGER.info(() -> String.format("Schema=%s Table=%s Payload=%s Message=%s Record=%s",
                    schemaName,
                    tableName,
                    payloads,
                    payloadsMessage, originRecord.getStoreValue()));
            for (TestBean payload : payloads) {
                if (this.entityManager.isJoinedToTransaction())
                    this.entityManager.merge(payload);
                if (this.entityManagerFactory.getCache() != null)
                    this.entityManagerFactory.getCache().evict(payload.getClass(), payload.getId());
            }
        }

        @Transactional
        @CanalListener(subscribes = "test\\.tb_canal_listener_test")
        public void batchListen(@Header(CanalMessageHeaderAccessor.SCHEMA_NAME) List<String> schemas,
                                @Header(CanalMessageHeaderAccessor.TABLE_NAME) List<String> tables,
                                @CanalBefore Collection<Collection<TestBean>> beforePayloads,
                                @Payload Collection<Collection<TestBean>> payloads,
                                Message<Collection<Collection<TestBean>>> payloadsMessages, CanalEntries<CanalEntry.Entry> originRecords) {
            LOGGER.info(() -> String.format("Schemas=%s Tables=%s BeforePayloads=%s AfterPayloads=%s Messages=%s Records=%s",
                    schemas,
                    tables,
                    beforePayloads,
                    payloads,
                    payloadsMessages, originRecords.stream().map(CanalEntry.Entry::getStoreValue).collect(Collectors.toList())));
            for (TestBean payload : payloads.stream().flatMap(Collection::stream).collect(Collectors.toList())) {
                if (this.entityManager.isJoinedToTransaction())
                    this.entityManager.merge(payload);
                if (this.entityManagerFactory.getCache() != null)
                    this.entityManagerFactory.getCache().evict(payload.getClass(), payload.getId());
            }
        }
    }

}
