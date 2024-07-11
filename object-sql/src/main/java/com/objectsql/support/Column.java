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

import com.objectsql.IMultiQuery;
import com.objectsql.IQuery;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Set;

public class Column implements Serializable {

    public static final String ALL = "*";

	private String name;//数据库表列字段名称
	private String function;//函数
	private String alias;//表别名
	private String asName;//字段别名

    private String fieldName;//用于查询

    //只有在 createQuery内部才有用
    private IQuery query;

    public IQuery getQuery() {
        return query;
    }

    public void setQuery(IQuery query) {
        this.query = query;
    }

    public Column(IQuery query){
        this.query = query;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

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

    public <T,R> Column(LambdaQuery<T,R> lambdaQuery){
        setName(lambdaQuery.getColumnName());
    }
		
	//u.num
    public Column(String alias, String name){
        setName(name);
        this.alias = alias;
    }

    public <T,R> Column(String alias, LambdaQuery<T,R> lambdaQuery){
        setName(lambdaQuery.getColumnName());
        this.alias = alias;
    }
		
	//u.num as big
	public Column(String alias, String name, String asName){
		this.alias = alias;
        setName(name);
        setAsName(asName);
	}

    public <T,R> Column(String alias, LambdaQuery<T,R> lambdaQuery, String asName){
        this.alias = alias;
        setName(lambdaQuery.getColumnName());
        setAsName(asName);
    }
	
	//sum(u.num) as big
    public Column(String function, String alias, String name, String asName){
        this.alias = alias;
        setFunction(function);
        setName(name);
        setAsName(asName);
    }

    public <T,R> Column(String function, String alias, LambdaQuery<T,R> lambdaQuery, String asName){
        this.alias = alias;
        setFunction(function);
        setName(lambdaQuery.getColumnName());
        setAsName(asName);
    }

    public Column all(){
        setFunction(Column.ALL);
        return this;
    }

    public Column sum(){
        setFunction(Function.SUM);
        return this;
    }

    public Column max(){
        setFunction(Function.MAX);
        return this;
    }

    public Column min(){
        setFunction(Function.MIN);
        return this;
    }

    public Column avg(){
        setFunction(Function.AVG);
        return this;
    }

    public Column count(){
        setFunction(Function.COUNT);
        return this;
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

    public <T,R> Column name(LambdaQuery<T,R> lambdaQuery){
        setName(lambdaQuery.getColumnName());
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

    public Column fieldName(String fieldName) {
        setFieldName(fieldName);
        return this;
    }

    public Expression equal(Object val){
        return new Expression(this, val, ExpressionType.CDT_EQUAL);
    }

    public Expression notEqual(Object val){
        return new Expression(this, val, ExpressionType.CDT_NOT_EQUAL);
    }

    public Expression like(Object val){
        return new Expression(this, val, ExpressionType.CDT_LIKE);
    }

    public Expression notLike(Object val){
        return new Expression(this, val, ExpressionType.CDT_NOT_LIKE);
    }

    public Expression startWith(Object val){
        return new Expression(this, val, ExpressionType.CDT_START_WITH);
    }

    public Expression notStartWith(Object val){
        return new Expression(this, val, ExpressionType.CDT_NOT_START_WITH);
    }

    public Expression endWith(Object val){
        return new Expression(this, val, ExpressionType.CDT_END_WITH);
    }

    public Expression notEndWith(Object val){
        return new Expression(this, val, ExpressionType.CDT_NOT_END_WITH);
    }

    public Expression less(Object val){
        return new Expression(this, val, ExpressionType.CDT_LESS);
    }

    public Expression lessEqual(Object val){
        return new Expression(this, val, ExpressionType.CDT_LESS_EQUAL);
    }

    public Expression more(Object val){
        return new Expression(this, val, ExpressionType.CDT_MORE);
    }

    public Expression moreEqual(Object val){
        return new Expression(this, val, ExpressionType.CDT_MORE_EQUAL);
    }

    public Expression in(Collection val){
        return new Expression(this, val, ExpressionType.CDT_IN);
    }

    public Expression notIn(Collection val){
        return new Expression(this, val, ExpressionType.CDT_NOT_IN);
    }

    public Expression isNull(){
        return new Expression(this, null, ExpressionType.CDT_IS_NULL);
    }

    public Expression isNotNull(){
        return new Expression(this, null, ExpressionType.CDT_IS_NOT_NULL);
    }

    public Expression isEmpty(){
        return new Expression(this, null, ExpressionType.CDT_IS_EMPTY);
    }

    public Expression isNotEmpty(){
        return new Expression(this, null, ExpressionType.CDT_IS_NOT_EMPTY);
    }

    public Expression exists(IMultiQuery val){
        return new Expression(this, val, ExpressionType.CDT_EXISTS);
    }

    public Expression notExists(IMultiQuery val){
        return new Expression(this, val, ExpressionType.CDT_NOT_EXISTS);
    }

    public Expression between(Object value1, Object value2){
        Expression ex = new Expression(this, value1, ExpressionType.CDT_BETWEEN);
        ex.andValue(value2);
        return ex;
    }

    public Order asc(){
        return new Order(this, Order.ASC);
    }

    public Order desc(){
        return new Order(this, Order.DESC);
    }

}