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

import org.springframework.canal.support.Acknowledgment;
import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;

import java.lang.reflect.Type;
import java.util.List;

/**
 * @author 橙子
 * @since 2020/12/30
 */
public abstract class AbstractCanalMessageConverter {
    protected void commonHeaders(CanalMessageHeaderAccessor headerAccessor, Acknowledgment acknowledgment, CanalConnector connector,
                               UnknownFieldSet unknownFields, Object entryType) {
        if (acknowledgment != null)
            headerAccessor.setAcknowledgment(acknowledgment);
        if (connector != null)
            headerAccessor.setConnector(connector);
        if (unknownFields != null)
            headerAccessor.setPayloadUnknownFieldSet(unknownFields);
        if (entryType instanceof CanalEntry.EntryType)
            headerAccessor.setEntryType((CanalEntry.EntryType) entryType);
        else if (entryType instanceof List)
            for (Object element : (List<?>) entryType)
                if (!(element instanceof CanalEntry.EntryType))
                    break;
                else
                    headerAccessor.appendEntryType((CanalEntry.EntryType) element);
    }

    protected abstract boolean canConvert(Class<?> entryClass, Type payloadType);
}
