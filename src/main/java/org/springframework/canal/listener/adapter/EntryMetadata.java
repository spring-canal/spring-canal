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

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.google.protobuf.UnknownFieldSet;

/**
 * @author 橙子
 * @since 2020/12/15
 */
public class EntryMetadata {
    private final CanalEntry.EntryType entryType;
    private final UnknownFieldSet unknownFields;
    private final int serializedSize;

    public EntryMetadata(CanalEntry.EntryType entryType, UnknownFieldSet unknownFields, int serializedSize) {
        this.entryType = entryType;
        this.unknownFields = unknownFields;
        this.serializedSize = serializedSize;
    }

    public CanalEntry.EntryType getEntryType() {
        return this.entryType;
    }

    public UnknownFieldSet getUnknownFields() {
        return this.unknownFields;
    }

    public int getSerializedSize() {
        return this.serializedSize;
    }
}
