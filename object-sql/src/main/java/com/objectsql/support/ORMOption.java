/*
 * Copyright 2017 @objectsql.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.objectsql.support;

import com.objectsql.utils.ORMUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORMOption implements Serializable {

    private Object original;
    private Object current;
    //更新全表
    private boolean updateNull;
    private String [] updateNullColumns;

    public Object getOriginal() {
        return original;
    }

    public void setOriginal(Object original) {
        this.original = original;
    }

    public Object getCurrent() {
        return current;
    }

    public void setCurrent(Object current) {
        this.current = current;
    }

    public ORMOption() {
    }

    public ORMOption(boolean updateNull, String[] updateNullColumns, Object original, Object current) {
        this.updateNull = updateNull;
        this.updateNullColumns = updateNullColumns;
        this.original = original;
        this.current = current;
    }

    public List<String> getColumnsWhenEmptyValueInNullColumns(){
        List<String> columns = new ArrayList<String>();
        List<ColumnInfo> infos = getColumnsInfoWhenEmptyValueInNullColumns();
        for (ColumnInfo info : infos){
            columns.add(info.getColumnName());
        }
        return columns;
    }

    public List<ColumnInfo> getColumnsInfoWhenEmptyValueInNullColumns(){
        List<String> nullColumns = new ArrayList<String>();
        if(updateNullColumns != null){
            nullColumns.addAll(Arrays.asList(updateNullColumns));
        }
        List<ColumnInfo> result = new ArrayList<ColumnInfo>();
        List<ColumnInfo> infos = ORMUtils.getColumnInfo(this.current.getClass());
        if (infos != null) {
            for (ColumnInfo info : infos) {
                if (!info.getPrimaryKey()) {
                    Object object = ORMUtils.getFieldValue(this.current, info);
                    if (object == null) {
                        if(this.updateNull || nullColumns.contains(info.getColumnName())) {
                            result.add(info);
                        }
                    }
                }
            }
        }
        return result;
    }

    public boolean isUpdateNull() {
        return updateNull;
    }

    public void setUpdateNull(boolean updateNull) {
        this.updateNull = updateNull;
    }

    public String[] getUpdateNullColumns() {
        return updateNullColumns;
    }

    public void setUpdateNullColumns(String[] updateNullColumns) {
        this.updateNullColumns = updateNullColumns;
    }
}