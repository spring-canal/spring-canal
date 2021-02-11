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

package org.springframework.canal.support.converter.record.header;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.UnknownFieldSet;

import org.springframework.canal.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageHeaderAccessor;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * See also {@link org.springframework.messaging.support.NativeMessageHeaderAccessor NativeMessageHeaderAccessor(store native simple string type send header)},
 * {@link org.springframework.messaging.simp.SimpMessageHeaderAccessor SimpMessageHeaderAccessor(generate message for {@link org.springframework.messaging.simp.SimpMessagingTemplate} send)}.
 *
 * @author 橙子
 * @since 2020/12/10
 */
public class CanalMessageHeaderAccessor extends MessageHeaderAccessor {
    /**
     * The prefix for canal message headers.
     */
    private static final String PREFIX = "canal_";
    /**
     * The header for holding the converted headers of the records; only provided
     * if listener is <strong>batch</strong> type.
     */
    public static final String CONVERTED_HEADER = PREFIX + "convertedHeaders";
    /**
     * The header for holding the native header(s) of the record; only provided
     * if no header mapper is present.
     */
    public static final String NATIVE_HEADER = PREFIX + "nativeHeader(s)";
    // --below are CanalEntry header fields--
    /**
     * The g_tid key; multi map values will turn to list type.
     */
    public static final String G_TID = PREFIX + "gTid(s)";
    /**
     * The executeTime key; multi map values will turn to list type.
     */
    public static final String EXECUTE_TIME = PREFIX + "executeTime(s)";
    /**
     * The eventType key; multi map values will turn to list type.
     */
    public static final String EVENT_TYPE = PREFIX + "eventType(s)";
    /**
     * The eventLength key; multi map values will turn to list type.
     */
    public static final String EVENT_LENGTH = PREFIX + "eventLength(s)";
    /**
     * The logfileName key; multi map values will turn to list type.
     */
    public static final String LOGFILE_NAME = PREFIX + "logfileName(s)";
    /**
     * The logfileOffset key; multi map values will turn to list type.
     */
    public static final String LOGFILE_OFFSET = PREFIX + "logfileOffset(s)";
    /**
     * The sourceType key; multi map values will turn to list type.
     */
    public static final String SOURCE_TYPE = PREFIX + "sourceType(s)";
    /**
     * The serverId key; multi map values will turn to list type.
     */
    public static final String SERVER_ID = PREFIX + "serverId(s)";
    /**
     * The serverCode key; multi map values will turn to list type.
     */
    public static final String SERVER_CODE = PREFIX + "serverCode(s)";
    /**
     * The schemaName key; multi map values will turn to list type.
     */
    public static final String SCHEMA_NAME = PREFIX + "schemaName(s)";
    /**
     * The tableName key; multi map values will turn to list type.
     */
    public static final String TABLE_NAME = PREFIX + "tableName(s)";
    /**
     * The version key; multi map values will turn to list type.
     */
    public static final String VERSION = PREFIX + "version(s)";
    /**
     * The props key; multi map values will turn to list type.
     */
    public static final String PROPERTIES = PREFIX + "props(s)";
    /**
     * The header for the header's {@link com.google.protobuf.UnknownFieldSet} object(s).
     */
    public static final String HEADER_UNKNOWN_FIELD_SET = PREFIX + "headerUnknownFieldSet(s)";
    // --below are CanalEntry payload fields--
    /**
     * The header for the {@link org.springframework.canal.support.Acknowledgment}.
     */
    public static final String ACKNOWLEDGMENT = PREFIX + "acknowledgment";
    /**
     * The header for the {@link com.alibaba.otter.canal.client.CanalConnector} object.
     */
    public static final String CONNECTOR = PREFIX + "connector";
    /**
     * The header for the {@link com.alibaba.otter.canal.protocol.CanalEntry.EntryType} object(s).
     */
    public static final String ENTRY_TYPE = PREFIX + "entryType(s)";
    /**
     * The header for the payload's {@link com.google.protobuf.UnknownFieldSet} object.
     */
    public static final String PAYLOAD_UNKNOWN_FIELD_SET = PREFIX + "payloadUnknownFieldSet";
    private boolean batchHeaderMutable = true;

    CanalMessageHeaderAccessor() {
    }

    CanalMessageHeaderAccessor(Message<?> message) {
        super(message);
    }

    public void setGTid(String gTid) {
        setHeader(CanalMessageHeaderAccessor.G_TID, gTid);
    }

    public String getGTid() {
        return getGTid(0);
    }

    public String getGTid(int index) {
        return (String) getHeader(CanalMessageHeaderAccessor.G_TID, index);
    }

    public void setExecuteTime(long executeTime) {
        setHeader(CanalMessageHeaderAccessor.EXECUTE_TIME, executeTime);
    }

    public Long getExecuteTime() {
        return getExecuteTime(0);
    }

    public Long getExecuteTime(int index) {
        return (Long) getHeader(CanalMessageHeaderAccessor.EXECUTE_TIME, index);
    }

    public void setEventType(CanalEntry.EventType eventType) {
        setHeader(CanalMessageHeaderAccessor.EVENT_TYPE, eventType);
    }

    public CanalEntry.EventType getEventType() {
        return getEventType(0);
    }

    public CanalEntry.EventType getEventType(int index) {
        return (CanalEntry.EventType) getHeader(CanalMessageHeaderAccessor.EVENT_TYPE, index);
    }

    public void setEventLength(long eventLength) {
        setHeader(CanalMessageHeaderAccessor.EVENT_LENGTH, eventLength);
    }

    public Long getEventLength() {
        return getEventLength(0);
    }

    public Long getEventLength(int index) {
        return (Long) getHeader(CanalMessageHeaderAccessor.EVENT_LENGTH, index);
    }

    public void setLogfileName(String logfileName) {
        setHeader(CanalMessageHeaderAccessor.LOGFILE_NAME, logfileName);
    }

    public String getLogfileName() {
        return getLogfileName(0);
    }

    public String getLogfileName(int index) {
        return (String) getHeader(CanalMessageHeaderAccessor.LOGFILE_NAME, index);
    }

    public void setLogfileOffset(long logfileOffset) {
        setHeader(CanalMessageHeaderAccessor.LOGFILE_OFFSET, logfileOffset);
    }

    public Long getLogfileOffset() {
        return getLogfileOffset(0);
    }

    public Long getLogfileOffset(int index) {
        return (Long) getHeader(CanalMessageHeaderAccessor.LOGFILE_OFFSET, index);
    }

    public void setSourceType(CanalEntry.Type sourceType) {
        setHeader(CanalMessageHeaderAccessor.SOURCE_TYPE, sourceType);
    }

    public CanalEntry.Type getSourceType() {
        return getSourceType(0);
    }

    public CanalEntry.Type getSourceType(int index) {
        return (CanalEntry.Type) getHeader(CanalMessageHeaderAccessor.SOURCE_TYPE, index);
    }

    public void setServerId(long serverId) {
        setHeader(CanalMessageHeaderAccessor.SERVER_ID, serverId);
    }

    public Long getServerId() {
        return getServerId(0);
    }

    public Long getServerId(int index) {
        return (Long) getHeader(CanalMessageHeaderAccessor.SERVER_ID, index);
    }

    public void setServerCode(String serverCode) {
        setHeader(CanalMessageHeaderAccessor.SERVER_CODE, serverCode);
    }

    public String getServerCode() {
        return getServerCode(0);
    }

    public String getServerCode(int index) {
        return (String) getHeader(CanalMessageHeaderAccessor.SERVER_CODE, index);
    }

    public void setSchemaName(String schemaName) {
        setHeader(CanalMessageHeaderAccessor.SCHEMA_NAME, schemaName);
    }

    public String getSchemaName() {
        return getSchemaName(0);
    }

    public String getSchemaName(int index) {
        return (String) getHeader(CanalMessageHeaderAccessor.SCHEMA_NAME, index);
    }

    public void setTableName(String tableName) {
        setHeader(CanalMessageHeaderAccessor.TABLE_NAME, tableName);
    }

    public String getTableName() {
        return getTableName(0);
    }

    public String getTableName(int index) {
        return (String) getHeader(CanalMessageHeaderAccessor.TABLE_NAME, index);
    }

    public void setVersion(int version) {
        setHeader(CanalMessageHeaderAccessor.VERSION, version);
    }

    public Integer getVersion() {
        return getVersion(0);
    }

    public Integer getVersion(int index) {
        return (Integer) getHeader(CanalMessageHeaderAccessor.VERSION, index);
    }

    public void setProperties(Map<String, String> properties) {
        setHeader(CanalMessageHeaderAccessor.PROPERTIES, properties);
    }

    public Map<String, String> getProperties() {
        return getProperties(0);
    }

    public Map<String, String> getProperties(int index) {
        return (Map<String, String>) getHeader(CanalMessageHeaderAccessor.PROPERTIES, index);
    }

    public void setHeaderUnknownFieldSet(UnknownFieldSet headerUnknownFieldSet) {
        setHeader(CanalMessageHeaderAccessor.HEADER_UNKNOWN_FIELD_SET, headerUnknownFieldSet);
    }

    public UnknownFieldSet getHeaderUnknownFieldSet() {
        return getHeaderUnknownFieldSet(0);
    }

    public UnknownFieldSet getHeaderUnknownFieldSet(int index) {
        return (UnknownFieldSet) getHeader(CanalMessageHeaderAccessor.HEADER_UNKNOWN_FIELD_SET, index);
    }

    public void setAcknowledgment(Acknowledgment acknowledgment) {
        setHeader(CanalMessageHeaderAccessor.ACKNOWLEDGMENT, acknowledgment);
    }

    public Acknowledgment getAcknowledgment() {
        return (Acknowledgment) getHeader(CanalMessageHeaderAccessor.ACKNOWLEDGMENT);
    }

    public void setConnector(CanalConnector connector) {
        setHeader(CanalMessageHeaderAccessor.CONNECTOR, connector);
    }

    public CanalConnector getConnector() {
        return (CanalConnector) getHeader(CanalMessageHeaderAccessor.CONNECTOR);
    }

    public void setEntryType(CanalEntry.EntryType entryType) {
        setHeader(CanalMessageHeaderAccessor.ENTRY_TYPE, entryType);
    }

    public CanalEntry.EntryType getEntryType() {
        return getEntryType(0);
    }

    public CanalEntry.EntryType getEntryType(int index) {
        return (CanalEntry.EntryType) getHeader(CanalMessageHeaderAccessor.ENTRY_TYPE, index);
    }

    public void setPayloadUnknownFieldSet(UnknownFieldSet payloadUnknownFieldSet) {
        setHeader(CanalMessageHeaderAccessor.PAYLOAD_UNKNOWN_FIELD_SET, payloadUnknownFieldSet);
    }

    public UnknownFieldSet getPayloadUnknownFieldSet() {
        return (UnknownFieldSet) getHeader(CanalMessageHeaderAccessor.PAYLOAD_UNKNOWN_FIELD_SET);
    }

    public void setNativeHeader(CanalEntry.Header nativeHeader) {
        setHeader(CanalMessageHeaderAccessor.NATIVE_HEADER, nativeHeader);
    }

    public CanalEntry.Header getNativeHeader() {
        return getNativeHeader(0);
    }

    public CanalEntry.Header getNativeHeader(int index) {
        return (CanalEntry.Header) getHeader(CanalMessageHeaderAccessor.NATIVE_HEADER, index);
    }

    public void setBatchHeaderMutable(boolean batchHeaderMutable) {
        this.batchHeaderMutable = batchHeaderMutable;
    }

    public boolean isBatchHeaderMutable() {
        return this.batchHeaderMutable;
    }

    public void appendEntryType(CanalEntry.EntryType entryType) {
        appendHeader(CanalMessageHeaderAccessor.ENTRY_TYPE, entryType);
    }

    public void removeBatchHeaders(int index) {
        Assert.state(this.batchHeaderMutable, "Already immutable");
        for (Object value : getMessageHeaders().values())
            if (value instanceof List && index < ((List<?>) value).size())
                ((List<?>) value).remove(index);
    }

    @Override
    protected void verifyType(String headerName, Object headerValue) {
        super.verifyType(headerName, headerValue);
        if (headerName != null && headerValue != null) {
            final Class<?>[] classes = CanalMessageHeaderAccessor.TYPE_MAP.get(headerName);
            if (classes != null) {
                for (Class<?> aClass : classes)
                    if (aClass.isInstance(headerValue))
                        return;
                throw new IllegalArgumentException("'" + headerName + "' header value's type must be one of " + Arrays.toString(classes));
            }
        }
    }

    @Override
    protected MessageHeaderAccessor createAccessor(Message<?> message) {
        return new CanalMessageHeaderAccessor(message);
    }

    private void appendHeader(String headerName, Object value) {
        Assert.state(this.batchHeaderMutable, "Already immutable");
        Object oldValue = getHeader(headerName);
        // fixme: element type cannot be List otherwise unboxing incorrectly
        if (!(oldValue instanceof List)) {
            oldValue = new ArrayList<>();
            setHeader(headerName, oldValue);
        }
        ((List<Object>) oldValue).add(value);
    }

    private Object getHeader(String name, int index) {
        final Object value = getHeader(name);
        // fixme: element type cannot be List otherwise unboxing incorrectly
        if (value instanceof List)
            return ((List<?>) value).isEmpty() ? null : ((List<?>) value).get(index);
        return value;
    }

    private static final Map<String, Class<?>[]> TYPE_MAP = new HashMap<>();

    static {
        final Class<?>[] intClasses = {List.class, Integer.class, Integer.TYPE};
        final Class<?>[] longClasses = {List.class, Long.class, Long.TYPE};
        final Class<?>[] stringClasses = {List.class, String.class};
        TYPE_MAP.put(CanalMessageHeaderAccessor.ACKNOWLEDGMENT, new Class[]{Acknowledgment.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.CONNECTOR, new Class[]{CanalConnector.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.PAYLOAD_UNKNOWN_FIELD_SET, new Class[]{UnknownFieldSet.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.ENTRY_TYPE, new Class[]{List.class, CanalEntry.EntryType.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.G_TID, stringClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.EXECUTE_TIME, longClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.EVENT_TYPE, new Class[]{List.class, CanalEntry.EventType.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.EVENT_LENGTH, longClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.LOGFILE_NAME, stringClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.LOGFILE_OFFSET, longClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.SOURCE_TYPE, new Class[]{List.class, CanalEntry.Type.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.SERVER_ID, longClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.SERVER_CODE, stringClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.SCHEMA_NAME, stringClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.TABLE_NAME, stringClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.VERSION, intClasses);
        TYPE_MAP.put(CanalMessageHeaderAccessor.PROPERTIES, new Class[]{List.class, Map.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.HEADER_UNKNOWN_FIELD_SET, new Class[]{List.class, UnknownFieldSet.class});
        TYPE_MAP.put(CanalMessageHeaderAccessor.NATIVE_HEADER, new Class[]{List.class, CanalEntry.Header.class});
    }

    public static CanalMessageHeaderAccessor create() {
        return new CanalMessageHeaderAccessor();
    }

    /**
     * @return null if not found.
     */
    public static CanalMessageHeaderAccessor getAccessor(Message<?> message) {
        return MessageHeaderAccessor.getAccessor(message, CanalMessageHeaderAccessor.class);
    }

    public static CanalMessageHeaderAccessor getMutableAccessor(Message<?> message) {
        final CanalMessageHeaderAccessor accessor = CanalMessageHeaderAccessor.getAccessor(message);
        if (accessor != null && accessor.isMutable())
            return accessor;
        return new CanalMessageHeaderAccessor(message);
    }
}
