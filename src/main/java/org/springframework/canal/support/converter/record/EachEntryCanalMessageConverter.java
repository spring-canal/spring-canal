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

package org.springframework.canal.support.converter.record;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import org.apache.commons.logging.LogFactory;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;
import org.springframework.canal.support.converter.record.header.mapper.CanalHeaderMapper;
import org.springframework.canal.support.converter.record.header.mapper.DefaultCanalHeaderMapper;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.IdTimestampMessageHeaderInitializer;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 橙子
 * @since 2020/12/9
 */
public class EachEntryCanalMessageConverter extends AbstractCanalMessageConverter implements EachCanalMessageConverter<CanalEntry.Entry> {
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
    private final IdTimestampMessageHeaderInitializer messageHeaderInitializer;
    private CanalHeaderMapper headerMapper = new DefaultCanalHeaderMapper();

    public EachEntryCanalMessageConverter() {
        this.messageHeaderInitializer = new IdTimestampMessageHeaderInitializer();
        this.messageHeaderInitializer.setDisableIdGeneration();
    }

    public void setHeaderMapper(CanalHeaderMapper headerMapper) {
        this.headerMapper = headerMapper;
    }

    /**
     * Generate {@link Message} {@code ids} for produced messages.
     * By default set to {@code false}.
     *
     * @param generateMessageId true if a message id should be generated
     */
    public void setGenerateMessageId(boolean generateMessageId) {
        if (generateMessageId)
            this.messageHeaderInitializer.setIdGenerator(null);
        else
            this.messageHeaderInitializer.setDisableIdGeneration();
    }

    /**
     * Generate {@code timestamp} for produced messages.
     * By default set to {@code false}.
     *
     * @param generateTimestamp true if a timestamp should be generated
     */
    public void setGenerateTimestamp(boolean generateTimestamp) {
        this.messageHeaderInitializer.setEnableTimestamp(generateTimestamp);
    }

    /**
     * {@inheritDoc}
     *
     * @return Type is {@link Message<com.google.protobuf.ByteString> Message&lt;ByteString>}.
     */
    @Override
    public Message<Object> toMessage(CanalEntry.Entry entry, Acknowledgment acknowledgment, CanalConnector connector, Type payloadType) {
        final Map<String, Object> messageHeaders = new HashMap<>();
        if (this.headerMapper != null && entry.hasHeader())
            this.headerMapper.toHeaders(entry.getHeader(), messageHeaders);
        else {
            this.logger.debug(() -> "No header mapper is available; headers (if present) are not mapped but provided raw in "
                    + CanalMessageHeaderAccessor.NATIVE_HEADER);
            messageHeaders.put(CanalMessageHeaderAccessor.NATIVE_HEADER, entry.getHeader());
        }
        CanalMessageHeaderAccessor headerAccessor = CanalMessageHeaderAccessor.create();
        headerAccessor.copyHeaders(messageHeaders);
        this.messageHeaderInitializer.initHeaders(headerAccessor);
        commonHeaders(headerAccessor, acknowledgment, connector, entry.getUnknownFields(), entry.hasEntryType() ? entry.getEntryType() : null);
        return MessageBuilder.createMessage(entry.getStoreValue(), headerAccessor.getMessageHeaders());
    }

    @Override
    public boolean canConvert(Class<?> entryClass, Type payloadType) {
        return CanalEntry.Entry.class.isAssignableFrom(entryClass);
    }
}
