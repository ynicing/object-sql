/*
 * Copyright 2017 @ursful.com
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
package com.ursful.framework.orm.support;



import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class QueryInfo implements Serializable {

	private String sql;
	private Class<?> clazz;//bean? String?
	Column column;//count?
	private List<Column> columns;
	private List<Pair> values;

	public Column getColumn() {
		return column;
	}
	public void setColumn(Column column) {
		this.column = column;
	}
	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		this.sql = sql;
	}
	public Class<?> getClazz() {
		return clazz;
	}
	public void setClazz(Class<?> clazz) {
		this.clazz = clazz;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	public List<Pair> getValues() {
		return values;
	}
	public void setValues(List<Pair> values) {
		this.values = values;
	}

    public String toString(){
//        String tmp = new String(sql);
//        if(values != null) {
//            for (Pair pair : values) {//Integer/String/Date
//                ClassType classType = ClassType.getClassType(pair.getValue().getClass().getSimpleName());
//                switch (classType) {
//                    case INTEGER:
//                    case FLOAT:
//                    case BYTE:
//                    case LONG:
//                    case DOUBLE:
//                        tmp = tmp.replaceFirst("\\?", pair.getValue() + "");
//                        break;
//                    case DATE:
//                    case TIMESTAMP:
//                        tmp = tmp.replaceFirst("\\?", "'" + DateUtils.getDate((Date)pair.getValue()) + "'");
//                        break;
//                    default:
//                        tmp = tmp.replaceFirst("\\?", "'" + pair.getValue() + "'");
//                }
//            }
//        }
        return "\n**********************************************************\n" +
                "SQL: " + sql + " : " + values;
    }
	
}
