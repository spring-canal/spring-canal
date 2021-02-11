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
import com.google.protobuf.UnknownFieldSet;
import org.apache.commons.logging.LogFactory;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.CanalEntries;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;
import org.springframework.canal.support.converter.record.header.mapper.CanalHeaderMapper;
import org.springframework.canal.support.converter.record.header.mapper.DefaultCanalHeaderMapper;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.IdTimestampMessageHeaderInitializer;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author 橙子
 * @since 2020/12/12
 */
public class BatchEntryCanalMessageConverter extends AbstractCanalMessageConverter implements BatchCanalMessageConverter<CanalEntry.Entry> {
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
    private final EachCanalMessageConverter<CanalEntry.Entry> eachMessageConverter;
    private final IdTimestampMessageHeaderInitializer messageHeaderInitializer;
    private CanalHeaderMapper headerMapper = new DefaultCanalHeaderMapper();

    public BatchEntryCanalMessageConverter() {
        this(null);
    }

    public BatchEntryCanalMessageConverter(EachCanalMessageConverter<CanalEntry.Entry> eachMessageConverter) {
        if (eachMessageConverter instanceof AbstractCanalMessageConverter
                && !((AbstractCanalMessageConverter) eachMessageConverter).canConvert(CanalEntry.Entry.class, CanalEntry.Entry.class))
            throw new IllegalArgumentException("Unsupported EachCanalMessageConverter that cannot convert no raw type: " + CanalEntry.Entry.class.getName());
        this.eachMessageConverter = eachMessageConverter;
        this.messageHeaderInitializer = new IdTimestampMessageHeaderInitializer();
        this.messageHeaderInitializer.setDisableIdGeneration();
    }

    public void setHeaderMapper(CanalHeaderMapper headerMapper) {
        this.headerMapper = headerMapper;
    }

    /**
     * Generate {@link Message} {@code ids} for produced messages. If set to {@code false},
     * will try to use a default value. By default set to {@code false}.
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
     * Generate {@code timestamp} for produced messages. If set to {@code false}, -1 is
     * used instead. By default set to {@code false}.
     *
     * @param generateTimestamp true if a timestamp should be generated
     */
    public void setGenerateTimestamp(boolean generateTimestamp) {
        this.messageHeaderInitializer.setEnableTimestamp(generateTimestamp);
    }

    /**
     * {@inheritDoc}
     *
     * @return Type is {@link Message<List<com.google.protobuf.ByteString>> Message&lt;List&lt;ByteString>>}.
     */
    @Override
    public Message<List<Object>> toMessage(CanalEntries<CanalEntry.Entry> entries, Acknowledgment acknowledgment, CanalConnector connector, Type payloadType) {
        final Map<String, List<Object>> messageHeaders = new HashMap<>();
        final Map<String, Object> entryHeaders = new HashMap<>();
        final UnknownFieldSet.Builder unknownFieldSetBuilder = UnknownFieldSet.newBuilder();
        final List<CanalEntry.EntryType> entryTypes = new ArrayList<>(entries.size());
        final List<Object> payloads = new ArrayList<>(entries.size());
        if (this.headerMapper == null)
            this.logger.debug(() -> "No header mapper is available; headers (if present) are not mapped but provided raw in "
                    + CanalMessageHeaderAccessor.NATIVE_HEADER);
        for (CanalEntry.Entry entry : entries) {
            if (this.headerMapper != null) {
                this.headerMapper.toHeaders(entry.hasHeader() ? entry.getHeader() : CanalEntry.Header.getDefaultInstance(), entryHeaders);
                entryHeaders.put(CanalMessageHeaderAccessor.CONVERTED_HEADER, entryHeaders);
            } else
                entryHeaders.put(CanalMessageHeaderAccessor.NATIVE_HEADER, entry.getHeader());
            for (Map.Entry<String, Object> entryHeader : entryHeaders.entrySet())
                messageHeaders.computeIfAbsent(entryHeader.getKey(), k -> new ArrayList<>()).add(entryHeader.getValue());
            unknownFieldSetBuilder.mergeFrom(entry.getUnknownFields());
            entryTypes.add(entry.hasEntryType() ? entry.getEntryType() : null);

            payloads.add(this.eachMessageConverter == null ? entry.getStoreValue() : this.eachMessageConverter.toMessage(entry, null, null, ((ParameterizedType) payloadType).getActualTypeArguments()[0]).getPayload());
        }
        CanalMessageHeaderAccessor headerAccessor = CanalMessageHeaderAccessor.create();
        headerAccessor.copyHeaders(messageHeaders);
        this.messageHeaderInitializer.initHeaders(headerAccessor);
        commonHeaders(headerAccessor, acknowledgment, connector, unknownFieldSetBuilder.build(), entryTypes);
        return MessageBuilder.createMessage(payloads, headerAccessor.getMessageHeaders());
    }

    @Override
    public boolean canConvert(Class<?> entryClass, Type payloadType) {
        return CanalEntry.Entry.class.isAssignableFrom(entryClass);
    }
}
