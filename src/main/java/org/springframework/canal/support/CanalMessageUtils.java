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

package org.springframework.canal.support;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;

import org.springframework.util.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author 橙子
 * @since 2020/12/23
 */
public final class CanalMessageUtils {
    /**
     * The invalid message id value.
     */
    public static final Long INVALID_MESSAGE_ID = -1L;
    private static final CanalEntryParser[] PARSERS;

    private CanalMessageUtils() {
    }

    public static boolean isEmpty(Message message) {
        if (message == null)
            return true;
        return message.isRaw() ? message.getRawEntries().isEmpty() : message.getEntries().isEmpty();
    }

    public static int getSize(Message message) {
        if (message == null)
            return 0;
        return message.isRaw() ? message.getRawEntries().size() : message.getEntries().size();
    }

    public static <T> CanalEntries<T> getEntries(Message message) {
        return new CanalEntries<>((List<T>) (message.isRaw() ? message.getRawEntries() : message.getEntries()), message.isRaw());
    }

    public static String toString(Message message, boolean metadataOnly) {
        if (message == null)
            return null;
        if (metadataOnly)
            return String.format("%d#%s", message.getId(), toString((message.isRaw() ? message.getRawEntries() : message.getEntries()), true));
        return message.toString();
    }

    public static <T> String toString(Collection<T> entriesData, boolean metadataOnly) {
        if (entriesData == null)
            return null;
        return entriesData.stream()
                .map(entry -> toString(entry, metadataOnly))
                .collect(Collectors.toList()).toString();
    }

    public static String toString(CanalEntry.Entry entryData, boolean metadataOnly) {
        if (entryData == null)
            return null;
        String tableName = entryData.getHeader().getTableName();
        if (StringUtils.isEmpty(tableName))
            tableName = "?";
        return !metadataOnly ? entryData.toString() : String.format("%s-%s@%s",
                tableName,
                entryData.getEntryType().name(),
                entryData.getHeader().getEventType().name());
    }

    public static <T> String toString(T entryData, boolean metadataOnly) {
        if (entryData == null)
            return null;
        try {
            for (CanalEntryParser parser : CanalMessageUtils.PARSERS)
                if (parser.canParse(entryData.getClass()))
                    return toString(parser.parseFrom(entryData), metadataOnly);
            return entryData.toString();
        } catch (IOException e) {
            return entryData.toString();
        }
    }

    static {
        PARSERS = new CanalEntryParser[]{
                new CanalEntryParser() {
                    @Override
                    public boolean canParse(Class<?> clazz) {
                        return byte[].class.isAssignableFrom(clazz);
                    }

                    @Override
                    public CanalEntry.Entry parseFrom(Object object) throws IOException {
                        return CanalEntry.Entry.parseFrom((byte[]) object);
                    }
                },
                new CanalEntryParser() {
                    @Override
                    public boolean canParse(Class<?> clazz) {
                        return ByteString.class.isAssignableFrom(clazz);
                    }

                    @Override
                    public CanalEntry.Entry parseFrom(Object object) throws IOException {
                        return CanalEntry.Entry.parseFrom((ByteString) object);
                    }
                },
                new CanalEntryParser() {
                    @Override
                    public boolean canParse(Class<?> clazz) {
                        return InputStream.class.isAssignableFrom(clazz);
                    }

                    @Override
                    public CanalEntry.Entry parseFrom(Object object) throws IOException {
                        return CanalEntry.Entry.parseFrom((InputStream) object);
                    }
                },
                new CanalEntryParser() {
                    @Override
                    public boolean canParse(Class<?> clazz) {
                        return CodedInputStream.class.isAssignableFrom(clazz);
                    }

                    @Override
                    public CanalEntry.Entry parseFrom(Object object) throws IOException {
                        return CanalEntry.Entry.parseFrom((CodedInputStream) object);
                    }
                },
                new CanalEntryParser() {
                    @Override
                    public boolean canParse(Class<?> clazz) {
                        return CanalEntry.Entry.class.isAssignableFrom(clazz);
                    }

                    @Override
                    public CanalEntry.Entry parseFrom(Object object) throws IOException {
                        return (CanalEntry.Entry) object;
                    }
                }
        };
    }

    private interface CanalEntryParser {
        boolean canParse(Class<?> clazz);

        CanalEntry.Entry parseFrom(Object object) throws IOException;
    }
}
