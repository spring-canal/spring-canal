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

package org.springframework.canal.listener.adapter;

import com.alibaba.otter.canal.common.CanalException;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.beans.BeanUtils;
import org.springframework.canal.annotation.CanalMessageHandlerMethodFactoryTest;
import org.springframework.canal.annotation.listener.CanalHandler;
import org.springframework.canal.annotation.listener.CanalListener;
import org.springframework.canal.listener.MessageListener;
import org.springframework.canal.support.CanalMessageUtils;
import org.springframework.canal.support.converter.message.CanalJpaMessageConverter;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.support.DefaultMessageHandlerMethodFactory;
import org.springframework.messaging.handler.invocation.InvocableHandlerMethod;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author 橙子
 * @since 2021/1/1
 */
class AbstractMessageListenerAdapterTest {
    private static final ResolvableType BYTE_STRING_COLLECTION = ResolvableType.forType(new ParameterizedTypeReference<Collection<ByteString>>() {
    });
    private static final ResolvableType CANAL_ENTRY_COLLECTION = ResolvableType.forType(new ParameterizedTypeReference<Collection<CanalEntry.Entry>>() {
    });
    private static final ResolvableType CANAL_ENTRY_LIST = ResolvableType.forType(new ParameterizedTypeReference<List<CanalEntry.Entry>>() {
    });
    private static final ResolvableType OBJECT_COLLECTION = ResolvableType.forType(new ParameterizedTypeReference<Collection<Object>>() {
    });
    private static final ResolvableType NUMBER_COLLECTION = ResolvableType.forType(new ParameterizedTypeReference<Collection<Number>>() {
    });
    private static final ResolvableType INTEGER_COLLECTION = ResolvableType.forType(new ParameterizedTypeReference<Collection<Integer>>() {
    });
    private static final ResolvableType ROW_CHANGE_MESSAGE = ResolvableType.forType(new ParameterizedTypeReference<Message<CanalEntry.RowChange>>() {
    });
    private static final ResolvableType BYTE_STRING_MESSAGE = ResolvableType.forType(new ParameterizedTypeReference<Message<ByteString>>() {
    });
    private static final ResolvableType CANAL_ENTRY_LIST_LIST = ResolvableType.forClassWithGenerics(List.class,
            CANAL_ENTRY_LIST);
    private static final ResolvableType CANAL_ENTRY_COLLECTION_LIST = ResolvableType.forClassWithGenerics(List.class,
            CANAL_ENTRY_COLLECTION);
    private static final ResolvableType CANAL_ENTRY_COLLECTION_COLLECTION = ResolvableType.forClassWithGenerics(Collection.class,
            CANAL_ENTRY_COLLECTION);
    private final CanalEntry.Entry EMPTY_ENTRY = CanalEntry.Entry.getDefaultInstance();
    private final ByteString EMPTY_RAW = ByteString.EMPTY;
    private final Object EMPTY_OBJECT = new Object();
    private EachMessageListenerAdapter<?> singleMethodEachListenerAdapter;
    private EachMessageListenerAdapter<?> multiMethodEachListenerAdapter;
    private BatchMessageListenerAdapter<?> multiMethodBatchListenerAdapter;

    @BeforeEach
    void setUp() {
        GenericApplicationContext simpleApplicationContext = new GenericApplicationContext();
        simpleApplicationContext.refresh();
        final SingleMethodListener singleMethodListener = BeanUtils.instantiateClass(SingleMethodListener.class);
        final DefaultMessageHandlerMethodFactory messageHandlerMethodFactory = CanalMessageHandlerMethodFactoryTest.getInstance(
                new CanalJpaMessageConverter(null, true));
        messageHandlerMethodFactory.afterPropertiesSet();
        InvocableHandlerMethod listenMethod = messageHandlerMethodFactory.createInvocableHandlerMethod(singleMethodListener,
                MethodIntrospector.selectMethods(singleMethodListener.getClass(), (MethodIntrospector.MetadataLookup<?>) method ->
                        AnnotatedElementUtils.findMergedAnnotation(method, CanalListener.class)).keySet().stream().findAny().get());
        this.singleMethodEachListenerAdapter = new EachMessageListenerAdapter<>(singleMethodListener, listenMethod);
        this.singleMethodEachListenerAdapter.setBeanFactory(simpleApplicationContext.getBeanFactory());
        this.singleMethodEachListenerAdapter.setFallbackPayloadType(String.class);
        final MultiMethodListener multiMethodListener = BeanUtils.instantiateClass(MultiMethodListener.class);
        Set<InvocableHandlerMethod> listenMethods = AnnotatedElementUtils.hasAnnotation(multiMethodListener.getClass(), CanalListener.class) ? MethodIntrospector.selectMethods(multiMethodListener.getClass(), (MethodIntrospector.MetadataLookup<?>) method ->
                AnnotatedElementUtils.findMergedAnnotation(method, CanalHandler.class)).keySet().stream()
                .map(method -> messageHandlerMethodFactory.createInvocableHandlerMethod(singleMethodListener, method))
                .collect(Collectors.toSet()) : Collections.emptySet();
        this.multiMethodEachListenerAdapter = new EachMessageListenerAdapter<CanalEntry.Entry>(multiMethodListener, null, new ArrayList<>(listenMethods), null);
        this.multiMethodEachListenerAdapter.setBeanFactory(simpleApplicationContext.getBeanFactory());
        this.multiMethodEachListenerAdapter.setFallbackPayloadType(String.class);
        this.multiMethodBatchListenerAdapter = new BatchMessageListenerAdapter<CanalEntry.Entry>(multiMethodListener, null, new ArrayList<>(listenMethods), null);
        this.multiMethodBatchListenerAdapter.setBeanFactory(simpleApplicationContext.getBeanFactory());
        this.multiMethodBatchListenerAdapter.setFallbackPayloadType(String.class);
    }

    @Test
    void findHandlerMethod() {
        assertEquals(this.singleMethodEachListenerAdapter.defaultHandlerMethod, this.singleMethodEachListenerAdapter.findHandlerMethod(CanalEntry.RowChange.getDefaultInstance()));
        assertEquals(this.singleMethodEachListenerAdapter.defaultHandlerMethod, this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY));
        assertEquals(this.singleMethodEachListenerAdapter.defaultHandlerMethod, this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_RAW));
        assertEquals(this.singleMethodEachListenerAdapter.defaultHandlerMethod, this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_OBJECT));
        assertEquals("listen1", this.multiMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY).getMethod().getName());
        assertEquals("listen2", this.multiMethodEachListenerAdapter.findHandlerMethod(EMPTY_RAW).getMethod().getName());
        assertThrows(CanalException.class, () -> this.multiMethodEachListenerAdapter.findHandlerMethod(Integer.MIN_VALUE),
                "Ambiguous methods for record:" + Integer.MIN_VALUE + " : listen5 and listen4");
        assertNull(this.multiMethodEachListenerAdapter.findHandlerMethod(EMPTY_OBJECT));
    }

    @Test
    void getParameterPayloadType() {
        this.singleMethodEachListenerAdapter.reset();
        assertEquals(CanalEntry.RowChange.class, this.singleMethodEachListenerAdapter.getParameterPayloadType(
                this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY)));
        this.singleMethodEachListenerAdapter.reset();
        assertEquals(CanalEntry.RowChange.class, this.singleMethodEachListenerAdapter.getParameterPayloadType(
                this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_RAW)));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(CanalEntry.RowChange.class, this.multiMethodEachListenerAdapter.getParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY)));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(ByteString.class, this.multiMethodEachListenerAdapter.getParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod(EMPTY_RAW)));
        // Fallback type
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(String.class, this.multiMethodEachListenerAdapter.getParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class, BYTE_STRING_MESSAGE.getType(), CANAL_ENTRY_COLLECTION.getType()));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(CANAL_ENTRY_COLLECTION.getType(), this.multiMethodEachListenerAdapter.getParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class, BYTE_STRING_MESSAGE.getType()));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(ByteString.class, this.multiMethodEachListenerAdapter.getParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class));
        // Fallback type
        this.multiMethodBatchListenerAdapter.reset();
        assertEquals(String.class, this.multiMethodBatchListenerAdapter.getParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class, BYTE_STRING_MESSAGE.getType(), CANAL_ENTRY_COLLECTION.getType()));
        // Fallback type
        this.multiMethodBatchListenerAdapter.reset();
        this.multiMethodBatchListenerAdapter.setRawEntry(false);
        assertEquals(String.class, this.multiMethodBatchListenerAdapter.getParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class, BYTE_STRING_MESSAGE.getType()));
        this.multiMethodBatchListenerAdapter.reset();
        this.multiMethodBatchListenerAdapter.setRawEntry(true);
        assertEquals(CanalEntry.Entry.class, this.multiMethodBatchListenerAdapter.getParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class, BYTE_STRING_MESSAGE.getType()));
        this.multiMethodBatchListenerAdapter.reset();
        assertEquals(ByteString.class, this.multiMethodBatchListenerAdapter.getParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("getParameterPayloadType"),
                CanalEntry.Entry.class));
    }

    @Test
    void extractParameterPayloadType() {
        assertEquals(CanalEntry.RowChange.class, this.singleMethodEachListenerAdapter.extractParameterPayloadType(
                this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY).getMethodParameters()[0]));
        assertEquals(ByteString.class, this.multiMethodEachListenerAdapter.extractParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("extractParameterPayloadType_Message<ByteString>").getMethodParameters()[0]));
        assertEquals(CANAL_ENTRY_COLLECTION.getType(), this.multiMethodEachListenerAdapter.extractParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("extractParameterPayloadType_Collection<CanalEntry.Entry>").getMethodParameters()[0]));
        assertNull(this.multiMethodEachListenerAdapter.extractParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("extractParameterPayloadType_Message<?>").getMethodParameters()[0]));
        assertEquals(Collection.class.getName() + "<?>", this.multiMethodEachListenerAdapter.extractParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("extractParameterPayloadType_Collection<?>").getMethodParameters()[0]).getTypeName());
        assertNull(this.multiMethodEachListenerAdapter.extractParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("extractParameterPayloadType_Message").getMethodParameters()[0]));
        assertEquals(Collection.class, this.multiMethodEachListenerAdapter.extractParameterPayloadType(
                this.multiMethodEachListenerAdapter.findHandlerMethod("extractParameterPayloadType_Collection").getMethodParameters()[0]));
        assertEquals(ByteString.class, this.multiMethodBatchListenerAdapter.extractParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("extractParameterPayloadType_Message<ByteString>").getMethodParameters()[0]));
        this.multiMethodBatchListenerAdapter.reset();
        this.multiMethodBatchListenerAdapter.setRawEntry(true);
        assertEquals(CanalEntry.Entry.class, this.multiMethodBatchListenerAdapter.extractParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("extractParameterPayloadType_Collection<CanalEntry.Entry>").getMethodParameters()[0]));
        assertNull(this.multiMethodBatchListenerAdapter.extractParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("extractParameterPayloadType_Message<?>").getMethodParameters()[0]));
        assertNull(this.multiMethodBatchListenerAdapter.extractParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("extractParameterPayloadType_Collection<?>").getMethodParameters()[0]));
        assertNull(this.multiMethodBatchListenerAdapter.extractParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("extractParameterPayloadType_Message").getMethodParameters()[0]));
        assertNull(this.multiMethodBatchListenerAdapter.extractParameterPayloadType(
                this.multiMethodBatchListenerAdapter.findHandlerMethod("extractParameterPayloadType_Collection").getMethodParameters()[0]));
    }

    @Test
    void getParameterSimilarTypeCount() {
        final Type rowChangeMessageType = ROW_CHANGE_MESSAGE.getType();
        final Type canalEntryCollectionType = CANAL_ENTRY_COLLECTION.getType();
        final Type byteStringCollectionType = BYTE_STRING_COLLECTION.getType();
        final Type objectCollectionType = OBJECT_COLLECTION.getType();
        final Type collectionNumberType = NUMBER_COLLECTION.getType();
        final Type collectionIntegerType = INTEGER_COLLECTION.getType();
        this.singleMethodEachListenerAdapter.reset();
        assertEquals(0, this.singleMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY),
                new Type[]{rowChangeMessageType},
                rowChangeMessageType));
        assertEquals(1, this.singleMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.singleMethodEachListenerAdapter.findHandlerMethod(EMPTY_ENTRY),
                rowChangeMessageType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(1, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType},
                canalEntryCollectionType));
        assertEquals(1, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                canalEntryCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(2, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType},
                Collection.class));
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                Collection.class));
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                List.class, Collection.class));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(1, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{List.class, Collection.class},
                CanalEntry.Entry.class));
        assertEquals(2, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                CanalEntry.Entry.class));
        assertEquals(1, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                ByteString.class));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(0, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType},
                CanalEntry.Entry.class));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(1, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType},
                collectionNumberType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(0, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType, collectionNumberType, collectionIntegerType, objectCollectionType, CanalEntry.Entry.class, ByteString.class},
                objectCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType, collectionNumberType, collectionIntegerType, objectCollectionType, CanalEntry.Entry.class},
                objectCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType, collectionNumberType, collectionIntegerType, objectCollectionType},
                objectCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType, collectionNumberType, collectionIntegerType},
                objectCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType, collectionNumberType},
                objectCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(4, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType, byteStringCollectionType},
                objectCollectionType));
        this.multiMethodEachListenerAdapter.reset();
        assertEquals(2, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                new Type[]{canalEntryCollectionType},
                objectCollectionType));
        assertEquals(6, this.multiMethodEachListenerAdapter.getParameterSimilarTypeCount(
                this.multiMethodEachListenerAdapter.findHandlerMethod("getParameterSimilarTypeCount"),
                objectCollectionType));
    }

    @Test
    void isSimilarTypeSimplify() {
        final Type rowChangeMessageType = ROW_CHANGE_MESSAGE.getType();
        final Type byteStringMessageType = BYTE_STRING_MESSAGE.getType();
        final Type canalEntryCollectionType = CANAL_ENTRY_COLLECTION.getType();
        final Type byteStringCollectionType = BYTE_STRING_COLLECTION.getType();
        final Type objectCollectionType = OBJECT_COLLECTION.getType();
        final Type canalEntryListType = CANAL_ENTRY_LIST.getType();
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(rowChangeMessageType, Message.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(rowChangeMessageType, byteStringMessageType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(byteStringMessageType, byteStringMessageType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(byteStringMessageType, Message.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(byteStringMessageType, rowChangeMessageType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionType, Collection.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionType, List.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionType, byteStringCollectionType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionType, objectCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionType, canalEntryListType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(byteStringCollectionType, Collection.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(byteStringCollectionType, List.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(byteStringCollectionType, canalEntryCollectionType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(byteStringCollectionType, objectCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(byteStringCollectionType, canalEntryListType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(objectCollectionType, Collection.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(objectCollectionType, List.class));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(objectCollectionType, canalEntryCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(objectCollectionType, byteStringCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(objectCollectionType, canalEntryListType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListType, List.class));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListType, Collection.class));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListType, canalEntryCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListType, byteStringCollectionType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListType, objectCollectionType));
    }

    @Test
    void isSimilarTypeComplicated() {
        final Type canalEntryCollectionType = CANAL_ENTRY_COLLECTION.getType();
        final Type canalEntryListType = CANAL_ENTRY_LIST.getType();
        final Type canalEntryListListType = CANAL_ENTRY_LIST_LIST.getType();
        final Type canalEntryCollectionListType = CANAL_ENTRY_COLLECTION_LIST.getType();
        final Type canalEntryCollectionCollectionType = CANAL_ENTRY_COLLECTION_COLLECTION.getType();
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListListType, canalEntryCollectionListType));
        assertTrue(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListListType, canalEntryCollectionCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListListType, canalEntryListType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryListListType, canalEntryCollectionType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionCollectionType, canalEntryCollectionListType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionCollectionType, canalEntryListListType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionCollectionType, canalEntryListType));
        assertFalse(this.singleMethodEachListenerAdapter.isSimilarType(canalEntryCollectionCollectionType, canalEntryCollectionType));
    }

    @SuppressWarnings("unchecked")
    @Test
    void onMessage() {
        ((MessageListener<Object>) this.singleMethodEachListenerAdapter).onMessage(
                CanalEntry.Entry.newBuilder()
                        .setHeader(CanalEntry.Header.newBuilder().setTableName("test"))
                        .setStoreValue(CanalEntry.RowChange.newBuilder()
                                .setSql("test1")
                                .build().toByteString())
                        .build(), null, null);
        ((MessageListener<Object>) this.singleMethodEachListenerAdapter).onMessage(
                CanalEntry.Entry.newBuilder()
                        .setStoreValue(CanalEntry.RowChange.newBuilder()
                                .setSql("test2")
                                .build().toByteString())
                        .build(), null, null);
        assertThrows(CanalException.class, () -> ((MessageListener<Object>) this.singleMethodEachListenerAdapter).onMessage(EMPTY_ENTRY, null, null));
        assertThrows(CanalException.class, () -> ((MessageListener<Object>) this.singleMethodEachListenerAdapter).onMessage(EMPTY_RAW, null, null));
        assertThrows(CanalException.class, () -> ((MessageListener<Object>) this.singleMethodEachListenerAdapter).onMessage(EMPTY_OBJECT, null, null));
    }

    private static class SingleMethodListener {
        private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(SingleMethodListener.class));

        @CanalListener
        public void listen(Message<CanalEntry.RowChange> message) {
            LOGGER.info(() -> String.format("Type=%s TableID=%d Sql=%s DdlSchema=%s Rows=%s",
                    message.getPayload().getEventType(),
                    message.getPayload().getTableId(),
                    message.getPayload().getSql(),
                    message.getPayload().getDdlSchemaName(),
                    message.getPayload().getRowDatasList()));
        }
    }

    @CanalListener
    private static class MultiMethodListener {
        private static final LogAccessor LOGGER = new LogAccessor(LogFactory.getLog(MultiMethodListener.class));

        @CanalHandler("#{__record instanceof T(com.alibaba.otter.canal.protocol.CanalEntry$Entry)}")
        public void listen1(Message<CanalEntry.RowChange> message) {
            LOGGER.info(() -> String.format("Type=%s TableID=%d Sql=%s", message.getPayload().getEventType(), message.getPayload().getTableId(), message.getPayload().getSql()));
        }

        @CanalHandler("#{__record instanceof T(com.google.protobuf.ByteString)}")
        public void listen2(Message<ByteString> message) {
            LOGGER.info(() -> CanalMessageUtils.toString(message.getPayload(), true));
        }

        @CanalHandler("#{__record == 'getParameterPayloadType'}")
        public void listen3(CanalEntry.Entry payload, Message<ByteString> message, Collection<CanalEntry.Entry> records) {
            LOGGER.info(() -> String.format("Payload=%s MessagePayload=%s Records=%s",
                    CanalMessageUtils.toString(payload, true),
                    CanalMessageUtils.toString(message.getPayload(), true),
                    records));
        }

        @CanalHandler("#{__record instanceof T(Number)}")
        public void listen4() {
        }

        @CanalHandler("#{__record instanceof T(Integer)}")
        public void listen5() {
        }

        @CanalHandler("#{__record == 'extractParameterPayloadType_Message<ByteString>'}")
        public void listen6(Message<ByteString> message) {
        }

        @CanalHandler("#{__record == 'extractParameterPayloadType_Collection<CanalEntry.Entry>'}")
        public void listen7(Collection<CanalEntry.Entry> records) {
        }

        @CanalHandler("#{__record == 'extractParameterPayloadType_Message<?>'}")
        public void listen8(Message<?> message) {
        }

        @CanalHandler("#{__record == 'extractParameterPayloadType_Collection<?>'}")
        public void listen9(Collection<?> records) {
        }

        @SuppressWarnings("rawtypes")
        @CanalHandler("#{__record == 'extractParameterPayloadType_Message'}")
        public void listen10(Message message) {
        }

        @SuppressWarnings("rawtypes")
        @CanalHandler("#{__record == 'extractParameterPayloadType_Collection'}")
        public void listen11(Collection records) {
        }

        @CanalHandler("#{__record == 'getParameterSimilarTypeCount'}")
        public void listen12(Collection<CanalEntry.Entry> records, Collection<ByteString> rawRecords1, List<ByteString> rawRecords2,
                             Collection<Number> multiExtra1, Collection<Integer> multiExtra2, Collection<?> multiExtra3,
                             CanalEntry.Entry singleExtra1, CanalEntry.Entry singleExtra2,
                             ByteString singleExtra3) {
        }
    }
}
