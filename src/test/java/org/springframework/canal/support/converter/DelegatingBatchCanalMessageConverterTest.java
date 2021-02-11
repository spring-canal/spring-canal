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

package org.springframework.canal.support.converter;

import com.alibaba.otter.canal.protocol.CanalEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.springframework.canal.support.CanalEntries;
import org.springframework.canal.support.converter.record.BatchCanalMessageConverter;
import org.springframework.canal.support.converter.record.BatchEntryCanalMessageConverter;
import org.springframework.canal.support.converter.record.BatchRawCanalMessageConverter;
import org.springframework.canal.support.converter.record.DelegatingBatchCanalMessageConverter;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author 橙子
 * @since 2020/12/17
 */
class DelegatingBatchCanalMessageConverterTest {
    private BatchCanalMessageConverter<?> batchCanalMessageConverter;

    @BeforeEach
    void beforeAll() {
        this.batchCanalMessageConverter = new DelegatingBatchCanalMessageConverter(
                new BatchEntryCanalMessageConverter(),
                new BatchRawCanalMessageConverter());
    }

    @Test
    void canalEntryToMessage() {
        Assertions.assertEquals(ArrayList.class.getGenericSuperclass().getClass(),
                ((BatchCanalMessageConverter<Object>) this.batchCanalMessageConverter)
                        .toMessage(new CanalEntries<>(Collections.singletonList(CanalEntry.Entry.getDefaultInstance()), false), null, null, null)
                        .getPayload()
                        .getClass().getGenericSuperclass().getClass());
    }

    @Test
    void byteStringToMessage() {
        Assertions.assertEquals(ArrayList.class.getGenericSuperclass().getClass(),
                ((BatchCanalMessageConverter<Object>) this.batchCanalMessageConverter)
                        .toMessage(new CanalEntries<>(Collections.singletonList(CanalEntry.Entry.getDefaultInstance().toByteString()), true), null, null, null)
                        .getPayload()
                        .getClass().getGenericSuperclass().getClass());
    }
}
