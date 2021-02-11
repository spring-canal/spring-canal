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

package org.springframework.canal.support.converter.message;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.LogFactory;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.persister.entity.EntityPersister;
import org.hibernate.persister.entity.OuterJoinLoadable;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.MutablePropertyValues;
import org.springframework.beans.PropertyAccessException;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.PropertyBatchUpdateException;
import org.springframework.canal.support.TypeUtils;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;
import org.springframework.core.ResolvableType;
import org.springframework.core.log.LogAccessor;
import org.springframework.messaging.Message;
import org.springframework.messaging.converter.SimpleMessageConverter;

import javax.persistence.EntityManagerFactory;
import javax.persistence.metamodel.EntityType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.lang.reflect.WildcardType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author 橙子
 * @since 2020/12/31
 */
public class CanalJpaMessageConverter extends SimpleMessageConverter implements CanalSmartMessageConverter {
    private static final List<?> SINGLE_NULL_LIST = Collections.singletonList(null);
    private static final ResolvableType CANAL_ROW_DATA_TYPE = ResolvableType.forClass(CanalEntry.RowData.class);
    private static final ResolvableType CANAL_COLUMN_TYPE = ResolvableType.forClass(CanalEntry.Column.class);
    protected final LogAccessor logger = new LogAccessor(LogFactory.getLog(getClass())); // NOSONAR
    private final EntityManagerFactory entityManagerFactory;
    private final boolean wholeParameterRequired;
    private boolean ignoreWholeNullList;

    public CanalJpaMessageConverter(EntityManagerFactory entityManagerFactory) {
        this(entityManagerFactory, false);
    }

    public CanalJpaMessageConverter(EntityManagerFactory entityManagerFactory, boolean wholeParameterRequired) {
        this.entityManagerFactory = entityManagerFactory;
        this.wholeParameterRequired = wholeParameterRequired;
        this.ignoreWholeNullList = true;
    }

    public boolean isIgnoreWholeNullList() {
        return this.ignoreWholeNullList;
    }

    public void setIgnoreWholeNullList(boolean ignoreWholeNullList) {
        this.ignoreWholeNullList = ignoreWholeNullList;
    }

    @Override
    public boolean isWholeParameterRequired() {
        return this.wholeParameterRequired;
    }

    @Override
    public Object fromMessage(Message<?> message, Class<?> targetClass, Object conversionHint) {
        if (this.wholeParameterRequired)
            return null;
        return CanalSmartMessageConverter.super.fromMessage(message, targetClass, conversionHint);
    }

    @Override
    public Object fromMessage(Message<?> message, Class<?> targetClass) {
        if (this.wholeParameterRequired)
            return null;
        return CanalSmartMessageConverter.super.fromMessage(message, targetClass);
    }

    @Override
    public Object fromMessage(Message<?> message, Type targetType, RowDataColumnsType rowDataColumnsType) {
        if (targetType instanceof TypeVariable)
            return fromMessage(message, TypeUtils.getBounds((TypeVariable<?>) targetType)[0], rowDataColumnsType);
        if (targetType instanceof WildcardType)
            return fromMessage(message, TypeUtils.getBothBounds((WildcardType) targetType)[0][0], rowDataColumnsType);
        if (targetType instanceof ParameterizedType)
            return fromMessage(message, (ParameterizedType) targetType, rowDataColumnsType);
        return convert(targetType, message.getHeaders().get(CanalMessageHeaderAccessor.TABLE_NAME, String.class),
                message.getPayload(), rowDataColumnsType);
    }

    protected Object convert(Type targetType, String tableName, Object payload, RowDataColumnsType rowDataColumnsType) {
        if (!(payload instanceof ByteString))
            return null;
        if (tableName == null)
            try {
                CanalEntry.Entry entry = CanalEntry.Entry.parseFrom((ByteString) payload);
                if (TypeUtils.isAssignable(CanalEntry.Entry.class, targetType))
                    return entry;
                tableName = entry.getHeader().getTableName();
                if (!entry.getStoreValue().isEmpty())
                    payload = entry.getStoreValue();
            } catch (InvalidProtocolBufferException ignored) {
                // Do nothing because of trying.
            }
        return doConvert(targetType, tableName, payload, rowDataColumnsType);
    }

    protected <T> Collection<T> getEntities(Class<T> targetClass, String tableName, List<CanalEntry.RowData> rowDataList, RowDataColumnsType rowDataColumnsType) {
        if (this.entityManagerFactory == null || tableName == null || rowDataList.isEmpty())
            return null;
        // fixme: Hard code related with Hibernate
        final EntityPersister entityPersister = this.entityManagerFactory.unwrap(SessionFactoryImplementor.class)
                .getMetamodel().locateEntityPersister(targetClass);
        if (!(entityPersister instanceof OuterJoinLoadable))
            throw new UnsupportedOperationException("Failed to locate entity persister of type: " + targetClass);
        final OuterJoinLoadable outerJoinLoadable = (OuterJoinLoadable) entityPersister;
        if (!outerJoinLoadable.getTableName().equals(tableName))
            throw new IllegalArgumentException("Unsupported table name: " + tableName + " expected: " + outerJoinLoadable.getTableName());
        // Usually size is 1
        if (rowDataList.size() == 1)
            return getSingleEntity(targetClass, rowDataList.get(0), rowDataColumnsType, outerJoinLoadable);
        return getMultiEntities(targetClass, rowDataList, rowDataColumnsType, outerJoinLoadable);
    }

    protected <T> T getEntity(Class<T> targetClass, Map<String, String> properties) {
        if (properties.isEmpty())
            return null;
        final BeanWrapper entityWrapper = PropertyAccessorFactory.forBeanPropertyAccess(BeanUtils.instantiateClass(targetClass));
        try {
            entityWrapper.setPropertyValues(new MutablePropertyValues(properties), true);
        } catch (PropertyBatchUpdateException propertiesException) {
            // Resolve foreign key
            for (PropertyAccessException propertyException : propertiesException.getPropertyAccessExceptions())
                try {
                    final String propertyName = Objects.requireNonNull(propertyException.getPropertyName());
                    final Class<?> propertyClass = BeanUtils.findPropertyType(propertyName, targetClass);
                    final EntityType<?> propertyEntityType = this.entityManagerFactory.getMetamodel().entity(propertyClass);
                    entityWrapper.setPropertyValue(propertyName, getEntity(propertyClass,
                            Collections.singletonMap(propertyEntityType.getId(propertyEntityType.getIdType().getJavaType()).getName(),
                                    (String) propertyException.getValue())));
                } catch (Exception e) {
                    e.addSuppressed(propertyException);
                    this.logger.warn(e, () -> "Failed to convert property " + propertyException.getPropertyName() + " as foreign key's object, continuing...");
                }
        }
        return (T) entityWrapper.getWrappedInstance();
    }

    private Object fromMessage(Message<?> message, ParameterizedType targetType, RowDataColumnsType rowDataColumnsType) {
        if (!(message.getPayload() instanceof Collection))
            return convert(targetType, message.getHeaders().get(CanalMessageHeaderAccessor.TABLE_NAME, String.class),
                    message.getPayload(), rowDataColumnsType);
        CanalMessageHeaderAccessor headerAccessor = CanalMessageHeaderAccessor.getAccessor(message);
        final List<Object> results = new ArrayList<>(((Collection<?>) message.getPayload()).size());
        try {
            Iterator<?> payloadIter = ((Collection<?>) message.getPayload()).iterator();
            int index = 0;
            while (payloadIter.hasNext()) {
                Object converted = convert(targetType.getActualTypeArguments()[0],
                        headerAccessor == null ? null : headerAccessor.getTableName(index),
                        payloadIter.next(),
                        rowDataColumnsType);
                if (converted != null) {
                    results.add(converted);
                    index++;
                } else if (headerAccessor != null && headerAccessor.isBatchHeaderMutable())
                    headerAccessor.removeBatchHeaders(index);
            }
            if (headerAccessor != null)
                headerAccessor.setBatchHeaderMutable(false);
        } catch (IndexOutOfBoundsException ignored) {
            // Do nothing because of it only happens when iterating after batch headers were removed.
        }
        return TypeUtils.isAssignable(Set.class, targetType.getRawType()) ? new HashSet<>(results) : results;
    }

    private Object doConvert(Type targetType, String tableName, Object payload, RowDataColumnsType rowDataColumnsType) {
        try {
            final CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom((ByteString) payload);
            if (TypeUtils.isAssignable(CanalEntry.RowChange.class, targetType))
                return rowChange;
            if (TypeUtils.isAssignable(CanalEntry.EventType.class, targetType))
                return rowChange.getEventType();
            // Collection<EntityType>
            ResolvableType resolvableType = ResolvableType.forType(targetType).asCollection().getGeneric();
            if (resolvableType == ResolvableType.NONE)
                return null;
            if (CANAL_ROW_DATA_TYPE.isAssignableFrom(resolvableType))
                return rowChange.getRowDatasList();
            if (CANAL_COLUMN_TYPE.isAssignableFrom(resolvableType))
                return rowChange.getRowDatasList().stream()
                        .map(rowDataColumnsType::getColumnsList)
                        .flatMap(Collection::stream)
                        .collect(Collectors.toList());
            if (CANAL_COLUMN_TYPE.isAssignableFrom(resolvableType.asCollection().getGeneric()))
                return rowChange.getRowDatasList().stream()
                        .map(rowDataColumnsType::getColumnsList)
                        .collect(Collectors.toList());
            return getEntities(resolvableType.resolve(), tableName, rowChange.getRowDatasList(), rowDataColumnsType);
        } catch (Exception e) {
            this.logger.error(e, () -> "Failed to convert payload " + payload + " to type " + targetType + ".");
        }
        return null;
    }

    private <T> Collection<T> getSingleEntity(Class<T> targetClass, CanalEntry.RowData rowData, RowDataColumnsType rowDataColumnsType,
                                              OuterJoinLoadable outerJoinLoadable) {
        final Map<String, String> columnPropertyMap = buildColumnPropertyMap(outerJoinLoadable);
        final List<CanalEntry.Column> columnsList = rowDataColumnsType.getColumnsList(rowData);
        final T entity = getEntity(targetClass, columnsList.isEmpty() ? Collections.emptyMap() : columnsList.stream()
                .collect(Collectors.toMap(column -> columnPropertyMap.get(column.getName()), CanalEntry.Column::getValue)));
        if (entity == null)
            return this.ignoreWholeNullList ? Collections.emptyList() : (Collection<T>) SINGLE_NULL_LIST;
        return Collections.singletonList(entity);
    }

    private <T> Collection<T> getMultiEntities(Class<T> targetClass, List<CanalEntry.RowData> rowDataList, RowDataColumnsType rowDataColumnsType,
                                               OuterJoinLoadable outerJoinLoadable) {
        final Map<String, String> columnPropertyMap = buildColumnPropertyMap(outerJoinLoadable);
        List<T> entities = null;
        int nullSize = 0;
        for (CanalEntry.RowData rowData : rowDataList) {
            final List<CanalEntry.Column> columnsList = rowDataColumnsType.getColumnsList(rowData);
            final T entity = getEntity(targetClass, columnsList.isEmpty() ? Collections.emptyMap() : columnsList.stream()
                    .collect(Collectors.toMap(column -> columnPropertyMap.get(column.getName()), CanalEntry.Column::getValue)));
            if (entities == null) {
                if (entity == null) {
                    nullSize++;
                    continue;
                }
                entities = new ArrayList<>(rowDataList.size());
                entities.addAll(Collections.nCopies(nullSize, null));
            }
            entities.add(entity);
        }
        if (nullSize == rowDataList.size())
            return this.ignoreWholeNullList ? Collections.emptyList() : Collections.nCopies(nullSize, null);
        return entities;
    }

    private Map<String, String> buildColumnPropertyMap(OuterJoinLoadable outerJoinLoadable) {
        final Map<String, String> columnPropertyMap = new HashMap<>(outerJoinLoadable.getIdentifierColumnNames().length + outerJoinLoadable.getPropertyNames().length);
        for (String columnName : outerJoinLoadable.getIdentifierColumnNames())
            columnPropertyMap.put(columnName, outerJoinLoadable.getIdentifierPropertyName());
        for (int i = 0; i < outerJoinLoadable.getPropertyNames().length; i++)
            for (String columnName : outerJoinLoadable.getPropertyColumnNames(i))
                columnPropertyMap.put(columnName, outerJoinLoadable.getPropertyNames()[i]);
        return columnPropertyMap;
    }
}

