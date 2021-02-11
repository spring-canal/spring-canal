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

import com.alibaba.otter.canal.protocol.CanalEntry;
import net.bytebuddy.utility.RandomString;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.canal.annotation.EnableCanal;
import org.springframework.canal.annotation.listener.CanalListener;
import org.springframework.canal.bootstrap.CanalBootstrapConfiguration;
import org.springframework.canal.config.ConsumerProperties;
import org.springframework.canal.config.ContainerProperties;
import org.springframework.canal.config.listener.container.CanalListenerContainerFactory;
import org.springframework.canal.config.listener.container.ConcurrencyCanalListenerContainerFactory;
import org.springframework.canal.core.CanalConnectorFactory;
import org.springframework.canal.core.SharedCanalConnectorFactory;
import org.springframework.canal.model.TestBean;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;
import org.springframework.canal.transaction.CanalTransactionManager;
import org.springframework.canal.transaction.ChainedCanalTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.persistence.PersistenceContext;
import java.util.Collection;
import java.util.Collections;

/**
 * @author 橙子
 * @since 2021/1/1
 */
@SpringJUnitConfig(classes = SpringApplicationTest.AppConfig.class)
class SpringApplicationTest {
    @Autowired
    private EntityManagerFactory entityManagerFactory;

    @BeforeEach
    void setUp() {
        prepareData();
    }

    @Test
    void start() throws InterruptedException {
        Assertions.assertNotNull(this.entityManagerFactory);
        synchronized (this) {
            wait(5000);
        }
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
        public CanalConnectorFactory connectorFactory(ObjectProvider<ConsumerProperties> properties) {
            return new SharedCanalConnectorFactory(properties.getIfUnique());
        }

        @Bean(CanalBootstrapConfiguration.CANAL_LISTENER_CONTAINER_FACTORY_BEAN_NAME)
        public CanalListenerContainerFactory<?> canalListenerContainerFactory(ObjectProvider<ContainerProperties> containerProperties,
                                                                              ObjectProvider<CanalConnectorFactory> canalConnectorFactory,
                                                                              ObjectProvider<PlatformTransactionManager> transactionManager) {
            final ConcurrencyCanalListenerContainerFactory<Object> canalListenerContainerFactory = new ConcurrencyCanalListenerContainerFactory<>();
            canalListenerContainerFactory.setContainerProperties(containerProperties.getIfUnique());
            canalListenerContainerFactory.setConnectorFactory(canalConnectorFactory.getIfAvailable());
            canalListenerContainerFactory.setTransactionManager(transactionManager.getIfUnique());
            return canalListenerContainerFactory;
        }

        @Bean
        public PlatformTransactionManager transactionManager(ObjectProvider<EntityManagerFactory> entityManagerFactory,
                                                             ObjectProvider<CanalConnectorFactory> canalConnectorFactory) {
            return new ChainedCanalTransactionManager(
                    new JpaTransactionManager(entityManagerFactory.getIfUnique()),
                    new CanalTransactionManager(canalConnectorFactory.getIfAvailable()));
        }

        @Bean
        public ContainerProperties containerProperties() {
            return new ContainerProperties();
        }

        @Bean
        public ConsumerProperties consumerProperties() {
            final ConsumerProperties consumerProperties = new ConsumerProperties();
            configConsumerProperties(consumerProperties);
            return consumerProperties;
        }

        @Bean
        public EntityManagerFactory entityManagerFactory() {
            return Persistence.createEntityManagerFactory("org.springframework.canal.listener");
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
    @EnableCanal
    static class ListenerConfig {
        private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(ListenerConfig.class));
        @Autowired
        private EntityManagerFactory entityManagerFactory;
        @PersistenceContext
        private EntityManager entityManager;

        @Transactional
        @CanalListener(subscribes = "test\\.tb_canal_listener_test", batchListener = "false")
        public void listen(@Header(CanalMessageHeaderAccessor.SCHEMA_NAME) String schemaName,
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
    }

}
