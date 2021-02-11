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

import java.util.List;

/**
 * @author 橙子
 * @since 2020/12/31
 */
public enum RowDataColumnsType {
    /**
     * The enum value of before columns list.
     */
    BEFORE(0) {
        @Override
        public List<CanalEntry.Column> getColumnsList(CanalEntry.RowData rowData) {
            return rowData.getBeforeColumnsList();
        }
    },
    /**
     * The enum value of after columns list.
     */
    AFTER(1) {
        @Override
        public List<CanalEntry.Column> getColumnsList(CanalEntry.RowData rowData) {
            return rowData.getAfterColumnsList();
        }
    };


    private final int value;

    RowDataColumnsType(int value) {
        this.value = value;
    }

    public final int getNumber() {
        return this.value;
    }

    public abstract List<CanalEntry.Column> getColumnsList(CanalEntry.RowData rowData);


    public static RowDataColumnsType valueOf(int num) {
        switch (num) {
            case 0:
                return BEFORE;
            case 1:
                return AFTER;
            default:
                return null;
        }
    }
}
