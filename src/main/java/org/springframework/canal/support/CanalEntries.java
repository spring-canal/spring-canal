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
import com.google.protobuf.ByteString;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Wrapper for interface {@link Collection<com.alibaba.otter.canal.protocol.CanalEntry.Entry> Collection&lt;CanalEntry.Entry>}
 * or {@link Collection<com.google.protobuf.ByteString> Collection&lt;ByteString>}
 * which suitable for Canal <strong>batch</strong> message listener method to
 * using payloads with type {@link Collection Collection&lt;PayloadType>} and origin records in the same time.
 * (Since in the method that {@link org.springframework.messaging.handler.invocation.InvocableHandlerMethod InvocableHandlerMethod}
 * find provided argument value according to parameter type,
 * the static method of {@link org.springframework.messaging.handler.HandlerMethod HandlerMethod} used first
 * ignore the difference between {@link Collection}'s generic type and cannot be overridden)
 *
 * @param <T> the entity type; can be {@link com.alibaba.otter.canal.protocol.CanalEntry CanalEntry.Entry} or {@link com.google.protobuf.ByteString ByteString}
 * @author 橙子
 * @since 2020/12/27
 */
public class CanalEntries<T> implements Iterable<T> {
    private final List<T> entries;
    private final Class<?> entryClass;

    public CanalEntries(List<T> entries) {
        this(entries, false);
    }

    public CanalEntries(List<T> entries, boolean isRaw) {
        this.entries = entries;
        this.entryClass = isRaw ? ByteString.class : CanalEntry.Entry.class;
    }

    public List<T> getEntries() {
        return this.entries;
    }

    public Class<?> getEntryClass() {
        return this.entryClass;
    }

    @Override
    public Iterator<T> iterator() {
        return this.entries.iterator();
    }

    public Stream<T> stream() {
        return this.entries.stream();
    }

    public boolean isEmpty() {
        return this.entries.isEmpty();
    }

    public int size() {
        return this.entries.size();
    }

    public void removeIf(Predicate<? super T> filter) {
        this.entries.removeIf(filter);
    }
}
