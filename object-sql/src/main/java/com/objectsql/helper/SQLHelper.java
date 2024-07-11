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
package com.objectsql.helper;

import com.objectsql.utils.ORMUtils;
import com.objectsql.support.Pair;
import org.springframework.validation.DataBinder;

import java.lang.reflect.Field;
import java.util.List;

public class SQLHelper {

	public Field getIdField() {
		return idField;
	}

    public Pair pair;

    public Pair getPair() {
        return pair;
    }

    public void setPair(Pair pair) {
        this.pair = pair;
    }

    public void setIdField(Field idField) {
		this.idField = idField;
	}

	private Field idField;
	private Object idValue;

	public Object getIdValue() {
		return idValue;
	}
	public void setIdValue(Object idValue) {
		this.idValue = idValue;
	}
	public void setId(Object object, Object value){
		if(value == null || object == null){
			return;
		}
		try {
			if(!idField.isAccessible()) {
				idField.setAccessible(true);
			}
			DataBinder binder = new DataBinder(idField, idField.getName());
			Object result = binder.convertIfNecessary(value.toString(), idField.getType());
			idField.set(object, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private String sql;
	private List<Pair> parameters;

	public String getSql() {
		return sql;
	}
	public void setSql(String sql) {
		ORMUtils.whenEmpty(sql, "SQL is null.");
		this.sql = ORMUtils.convertSQL(sql);
	}
	public List<Pair> getParameters() {
		return parameters;
	}
	public void setParameters(List<Pair> parameters) {
		this.parameters = parameters;
	}
	
	public String toString(){
		return "\n**********************************************************\nSQL: " + sql + " : " + parameters;
	}
	
	
}
