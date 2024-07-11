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

import com.objectsql.annotation.RdColumn;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

//name,type,value,fieldName
//
public class Pair implements Serializable{
	 
	private static final long serialVersionUID = -1L;
	
	private String name;//fieldName

    private String type;//fieldType

    private Object value;//fieldValue

    private String column;//column_names

    private ColumnType columnType;

    private Map<String, Object> metadata;

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public <T,R> void setColumn(LambdaQuery<T,R> lambdaQuery) {
        this.column = lambdaQuery.getColumnName();
    }

    public Pair(ColumnInfo info, Object value){
        this.value = value;
        if(info != null) {
            this.name = info.getName();
            this.columnType = info.getColumnType();
            this.type = info.getType();
            this.column = info.getColumnName();
            if(info.getField() != null) {
                RdColumn column = info.getField().getAnnotation(RdColumn.class);
                if(column != null) {
                    Map<String, Object> data = new HashMap<String, Object>();
                    data.put("runningMode", column.runningMode());
                    data.put("scale", column.scale());
                    data.put("precision", column.precision());
                    data.put("coding", column.coding());
                    this.metadata = data;
                }
            }
        }
    }

    public Pair(String name, Object value){
        this.name = name;
        this.value = value;
        if(value != null){
            this.type = value.getClass().getSimpleName();
        }
    }

    public Pair(Object value){
        this.value = value;
        if(value != null){
            this.type = value.getClass().getSimpleName();
        }
    }

    public Pair(Object value, ColumnType columnType){
        this.value = value;
        if(value != null){
            this.type = value.getClass().getSimpleName();
        }
        this.columnType = columnType;
    }

	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String toString(){
		return "{" + ((column == null || "".equals(column))?"": column + ":") + (((value instanceof Date) && (this.columnType == ColumnType.LONG))?((value instanceof Long)?value:((Date) value).getTime()):value) + "}";
	}
	
}
