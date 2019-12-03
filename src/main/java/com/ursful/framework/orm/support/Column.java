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
import java.util.Locale;

public class Column implements Serializable {

	private String name;//数据库表列字段名称
	private String function;//函数
	private String alias;//表别名
	private String asName;//字段别名

    private Object value;
    private OperatorType type;

    private String format;

    private Boolean operatorInFunction = true;

    public Boolean getOperatorInFunction() {
        return operatorInFunction;
    }

    public void setOperatorInFunction(Boolean operatorInFunction) {
        this.operatorInFunction = operatorInFunction;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public OperatorType getType() {
        return type;
    }

    public void setType(OperatorType type) {
        this.type = type;
    }

    public String getAsName() {
		return asName;
	}

	public void setAsName(String asName) {
        if(asName != null) {
            this.asName = getReplace(asName.toUpperCase(Locale.ROOT));
        }
	}

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getFunction() {
		return function;
	}

	public void setFunction(String function) {
        if(function != null) {
            this.function = getReplace(function);
        }
	}

	public String getName() {
		return name;
	}

    public String getReplace(String value){
        if(value != null){
            value = value.replace(" ", "");
        }
        return value;
    }

	public void setName(String name) {
		this.name = getReplace(name);
	}

	//---num
	//count(num)
	//count(num) as n
	//x count(a.num)
	//--a.num
	//--a.num as n
	//--count(a.num) as n
	
	//count(num) as n
	/*public Column(StringBuffer function, String name, String asName){
		this.function = function.toString();
		this.name = name;
		this.asName = asName;
	}*/

    public Column(){

    }


	//num
	public Column(String name){
        setName(name);
	}
		
	//u.num
	public Column(String alias, String name){
		setName(name);
		this.alias = alias;
	}
		
	//u.num as big
	public Column(String alias, String name, String asName){
		this.alias = alias;
        setName(name);
        setAsName(asName);
	}
	
	//sum(u.num) as big
	public Column(String function, String alias, String name, String asName){
		this.alias = alias;
        setFunction(function);
        setName(name);
        setAsName(asName);
	}

    public Column function(String function){
        setFunction(function);
        return this;
    }

    public Column alias(String alias){
        this.alias = alias;
        return this;
    }

    public Column name(String name){
        setName(name);
        return this;
    }

    public Column as(String asName){
        setAsName(asName);
        return this;
    }

    public Column value(Object value){
        this.value = value;
        return this;
    }

    public Column operator(OperatorType type){
        this.type = type;
        return this;
    }

//    String badStr = "'|and|exec|execute|insert|select|delete|update|count|drop|*|%|chr|mid|master|truncate|" +
//            "char|declare|sitename|net user|xp_cmdshell|;|or|-|+|,|like'|and|exec|execute|insert|create|drop|" +
//            "table|from|grant|use|group_concat|column_name|" +
//            "information_schema.columns|table_schema|union|where|select|delete|update|order|by|count|*|" +
//            "chr|mid|master|truncate|char|declare|or|;|-|--|+|,|like|//|/|%|#";

    public Column format(String format){
        this.format = format;
        return this;
    }

    public Column formatValues(Object ... values){
        this.value = values;
        return this;
    }

    public Column field(String field){
        if(field == null || field.length() == 0){
            return this;
        }

        StringBuffer sb = new StringBuffer();

        for(int i = 0; i < field.length(); i++){
            char c = field.charAt(i);
            if('A' <= c && c <= 'Z'){
                sb.append("_");
                sb.append((c+"").toLowerCase(Locale.ROOT));
            }else{
                sb.append(c);
            }
        }
        this.name = sb.toString();

        return this;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public Column operatorInFunction(Boolean operatorInFunction) {
        this.operatorInFunction = operatorInFunction;
        return this;
    }

    public static void main(String[] args) {
        //Column c = new Column().function("sum").alias("u").name("test").as("x");
    }
}