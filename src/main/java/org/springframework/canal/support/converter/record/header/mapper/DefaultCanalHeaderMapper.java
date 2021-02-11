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

package org.springframework.canal.support.converter.record.header.mapper;

import com.alibaba.otter.canal.protocol.CanalEntry;

import org.springframework.canal.support.converter.record.header.CanalMessageHeaderAccessor;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 橙子
 * @since 2020/12/11
 */
public class DefaultCanalHeaderMapper implements CanalHeaderMapper {
    @Override
    public void toHeaders(CanalEntry.Header src, Map<String, Object> dest) {
        dest.put(CanalMessageHeaderAccessor.G_TID, src.hasGtid() ? src.getGtid() : null);
        dest.put(CanalMessageHeaderAccessor.EXECUTE_TIME, src.hasExecuteTime() ? src.getExecuteTime() : null);
        dest.put(CanalMessageHeaderAccessor.EVENT_TYPE, src.hasEventType() ? src.getEventType() : null);
        dest.put(CanalMessageHeaderAccessor.EVENT_LENGTH, src.hasEventLength() ? src.getEventLength() : null);
        dest.put(CanalMessageHeaderAccessor.LOGFILE_NAME, src.hasLogfileName() ? src.getLogfileName() : null);
        dest.put(CanalMessageHeaderAccessor.LOGFILE_OFFSET, src.hasLogfileOffset() ? src.getLogfileOffset() : null);
        dest.put(CanalMessageHeaderAccessor.SOURCE_TYPE, src.hasSourceType() ? src.getSourceType() : null);
        dest.put(CanalMessageHeaderAccessor.SERVER_ID, src.hasServerId() ? src.getServerId() : null);
        dest.put(CanalMessageHeaderAccessor.SERVER_CODE, src.hasServerenCode() ? src.getServerenCode() : null);
        dest.put(CanalMessageHeaderAccessor.SCHEMA_NAME, src.hasSchemaName() ? src.getSchemaName() : null);
        dest.put(CanalMessageHeaderAccessor.TABLE_NAME, src.hasTableName() ? src.getTableName() : null);
        dest.put(CanalMessageHeaderAccessor.VERSION, src.hasVersion() ? src.getVersion() : null);
        Map<String, String> propertiesMap = new HashMap<>(src.getPropsCount());
        for (CanalEntry.Pair pair : src.getPropsList())
            propertiesMap.put(pair.getKey(), pair.getValue());
        dest.put(CanalMessageHeaderAccessor.PROPERTIES, propertiesMap);
        dest.put(CanalMessageHeaderAccessor.HEADER_UNKNOWN_FIELD_SET, src.getUnknownFields());
    }
}
