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

import java.io.Serializable;
import java.util.Collection;

public class Expression implements Serializable {

	@Deprecated
    public static final String EXPRESSION_ALL = "*";
	@Deprecated
    public static final String EXPRESSION_SUM = "SUM";
	@Deprecated
    public static final String EXPRESSION_MAX = "MAX";
	@Deprecated
    public static final String EXPRESSION_MIN = "MIN";
	@Deprecated
    public static final String EXPRESSION_AVG = "AVG";
	@Deprecated
    public static final String EXPRESSION_COUNT = "COUNT";

    private Column left;
	private ExpressionType type;
	private Object value;
	private Object andValue;

	public Expression andValue(Object andValue) {
		this.andValue = andValue;
		return this;
	}

	public Object getAndValue() {
		return andValue;
	}

	public void setAndValue(Object andValue) {
		this.andValue = andValue;
	}

	//exists or not exists
	public Expression(ExpressionType type, IMultiQuery value){
		this.type = type;
		this.value = value;
	}


	public Expression(Column left, Column value){
		this.left = left;
		this.type = ExpressionType.CDT_EQUAL;
		this.value = value;
	}

    public Expression(Column left, ExpressionType type){
        this.left = left;
        this.type = type;
    }

	public Expression(Column left, Object value, ExpressionType type){
		this.left = left;
		this.type = type;
		this.value = value;
	}

	public Expression(String left, Object value, ExpressionType type){
		this.left = new Column(left);
		this.type = type;
		this.value = value;
	}

	public <T,R> Expression(LambdaQuery<T,R> lambdaQuery, Object value, ExpressionType type){
		this(lambdaQuery.getColumnName(), value, type);
	}

	public Expression(String function, String left, Object value, ExpressionType type){
		this.left = new Column(function, null, left, null);
		this.type = type;
		this.value = value;
	}

	public <T,R> Expression(String function, LambdaQuery<T,R> lambdaQuery, Object value, ExpressionType type){
		this(function, lambdaQuery.getColumnName(), value, type);
	}

	public Expression(String left, ExpressionType type){
		this.left = new Column(left);
		this.type = type;
	}

	public <T,R> Expression(LambdaQuery<T,R> lambdaQuery, ExpressionType type){
		this(lambdaQuery.getColumnName(), type);
	}

	public Expression(String left, Object value){
		this.left = new Column(left);
		if(value instanceof ExpressionType){
			this.type = (ExpressionType)value;
		}else {
			this.type = ExpressionType.CDT_EQUAL;
			this.value = value;
		}
	}

	public <T,R> Expression(LambdaQuery<T,R> lambdaQuery, Object value){
		this(lambdaQuery.getColumnName(), value);
	}

	public Column getLeft() {
		return left;
	}

    /**
     * set
     * @param left
     */
	public void setLeft(Column left) {
		this.left = left;
	}

	public ExpressionType getType() {
		return type;
	}
	public void setType(ExpressionType type) {
		this.type = type;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}

	public static Expression equal(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_EQUAL);
	}

	public static <T,R> Expression equal(LambdaQuery<T,R> lambdaQuery, Object val) {
		return equal(lambdaQuery.getColumnName(), val);
	}

	public static  Expression notEqual(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_NOT_EQUAL);
	}

	public static <T,R> Expression notEqual(LambdaQuery<T,R> lambdaQuery, Object val) {
		return notEqual(lambdaQuery.getColumnName(), val);
	}

	public static  Expression like(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_LIKE);
	}

	public static <T,R> Expression like(LambdaQuery<T,R> lambdaQuery, Object val) {
		return like(lambdaQuery.getColumnName(), val);
	}

	public static  Expression notLike(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_NOT_LIKE);
	}

	public static <T,R> Expression notLike(LambdaQuery<T,R> lambdaQuery, Object val) {
		return notLike(lambdaQuery.getColumnName(), val);
	}

	public static  Expression startWith(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_START_WITH);
	}

	public static <T,R> Expression startWith(LambdaQuery<T,R> lambdaQuery, Object val) {
		return startWith(lambdaQuery.getColumnName(), val);
	}

	public static  Expression notStartWith(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_NOT_START_WITH);
	}

	public static <T,R> Expression notStartWith(LambdaQuery<T,R> lambdaQuery, Object val) {
		return notStartWith(lambdaQuery.getColumnName(), val);
	}

	public static  Expression endWith(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_END_WITH);
	}

	public static <T,R> Expression endWith(LambdaQuery<T,R> lambdaQuery, Object val) {
		return endWith(lambdaQuery.getColumnName(), val);
	}

	public static  Expression notEndWith(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_NOT_END_WITH);
	}

	public static <T,R> Expression notEndWith(LambdaQuery<T,R> lambdaQuery, Object val) {
		return notEndWith(lambdaQuery.getColumnName(), val);
	}

	public static  Expression less(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_LESS);
	}

	public static <T,R> Expression less(LambdaQuery<T,R> lambdaQuery, Object val) {
		return less(lambdaQuery.getColumnName(), val);
	}

	public static  Expression lessEqual(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_LESS_EQUAL);
	}

	public static <T,R> Expression lessEqual(LambdaQuery<T,R> lambdaQuery, Object val) {
		return lessEqual(lambdaQuery.getColumnName(), val);
	}

	public static  Expression more(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_MORE);
	}

	public static <T,R> Expression more(LambdaQuery<T,R> lambdaQuery, Object val) {
		return more(lambdaQuery.getColumnName(), val);
	}

	public static  Expression moreEqual(String column, Object val){
		return new Expression(new Column(column), val, ExpressionType.CDT_MORE_EQUAL);
	}

	public static <T,R> Expression moreEqual(LambdaQuery<T,R> lambdaQuery, Object val) {
		return moreEqual(lambdaQuery.getColumnName(), val);
	}

	public static  Expression in(String column, Collection val){
		return new Expression(new Column(column), val, ExpressionType.CDT_IN);
	}

	public static <T,R> Expression in(LambdaQuery<T,R> lambdaQuery, Collection val) {
		return in(lambdaQuery.getColumnName(), val);
	}

	public static  Expression notIn(String column, Collection val){
		return new Expression(new Column(column), val, ExpressionType.CDT_NOT_IN);
	}

	public static <T,R> Expression notIn(LambdaQuery<T,R> lambdaQuery, Collection val) {
		return notIn(lambdaQuery.getColumnName(), val);
	}

	public static  Expression isNull(String column){
		return new Expression(new Column(column), null, ExpressionType.CDT_IS_NULL);
	}

	public static <T,R> Expression isNull(LambdaQuery<T,R> lambdaQuery) {
		return isNull(lambdaQuery.getColumnName());
	}

	public static  Expression isNotNull(String column){
		return new Expression(new Column(column), null, ExpressionType.CDT_IS_NOT_NULL);
	}

	public static <T,R> Expression isNotNull(LambdaQuery<T,R> lambdaQuery) {
		return isNotNull(lambdaQuery.getColumnName());
	}

	public static  Expression isEmpty(String column){
		return new Expression(new Column(column), null, ExpressionType.CDT_IS_EMPTY);
	}

	public static <T,R> Expression isEmpty(LambdaQuery<T,R> lambdaQuery) {
		return isEmpty(lambdaQuery.getColumnName());
	}

	public static  Expression isNotEmpty(String column){
		return new Expression(new Column(column), null, ExpressionType.CDT_IS_NOT_EMPTY);
	}

	public static <T,R> Expression isNotEmpty(LambdaQuery<T,R> lambdaQuery) {
		return isNotEmpty(lambdaQuery.getColumnName());
	}

	public static Expression between(String column, Object value1, Object value2){
		Expression ex = new Expression(new Column(column), value1, ExpressionType.CDT_BETWEEN);
		ex.andValue(value2);
		return ex;
	}

	public static <T,R> Expression between(LambdaQuery<T,R> lambdaQuery, Object value1, Object value2){
		return between(lambdaQuery.getColumnName(), value1, value2);
	}
}