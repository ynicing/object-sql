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


public enum DataType {
	/**
	 * 默认都是String，因为是Http请求！
	 */
	VOID("v", "V", "void", "VOID"),
	BOOLEAN("z","Z","boolean", "BOOLEAN", "java.lang.Boolean"),
	CHAR("c","C","char", "Character", "java.lang.Character"),
	BYTE("b","B","byte", "Byte", "java.lang.Byte"),
	SHORT("s","S","short", "Short", "java.lang.Short"),
	INTEGER("i","I","int", "Integer", "java.lang.Integer"),
	FLOAT("f","F","float", "Float", "java.lang.Float"),
	LONG("j","J","long", "Long", "java.lang.Long"),
	DOUBLE("d","D","double","Double","java.lang.Double","java.math.BigDecimal"),
	DATE("Date","java.util.Date"),
    MAP("Map","java.util.Map", "java.util.HashMap"),
//    LINKED_MAP("LinkedHashMap","java.util.LinkedHashMap"),
	TIMESTAMP("Timestamp","java.sql.Timestamp"),
	STRING("String", "java.lang.String"),
	OBJECT("Object","java.lang.Object"),
	BINARY("Byte[]","byte[]"),
	DECIMAL("BigDecimal", "java.math.BigDecimal"),
	UNKNOWN("UNKOWN");

	private String [] types;

	public String[] getTypes() {
		return types;
	}
	public void setTypes(String[] types) {
		this.types = types;
	}
	DataType(String... types){
		this.types = types;
	}
  
	public static DataType getDataType(String type){
		DataType result = UNKNOWN;
		first:for(DataType dt : DataType.values()){
			String[] types = dt.getTypes();
			for(String t : types){
				if(t.equals(type)){
					result = dt;
					break first;
				}
			}
		}
		return result;
	}

}
