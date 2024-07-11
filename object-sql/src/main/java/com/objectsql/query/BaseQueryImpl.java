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
package com.objectsql.query;

import com.objectsql.support.*;
import com.objectsql.IBaseQuery;
import com.objectsql.annotation.RdTable;
import com.objectsql.utils.ORMUtils;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.*;

public class BaseQueryImpl extends QueryImpl implements IBaseQuery {

    private Class<?> table;
    private Class<?> returnClass;
    private List<Column> returnColumns = new ArrayList<Column>();
    private List<Column> fixedReturnColumns = new ArrayList<Column>();

    private List<Column> groups = new ArrayList<Column>();
    private List<Column> groupCountsSelectColumns = new ArrayList<Column>();

    public IBaseQuery orderDesc(String name){
		orders.add(new Order(new Column(name), Order.DESC));
		return this;
	}

    public IBaseQuery whereIsNull(String name){
        addCondition(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_NULL)));
        return this;
    }

    public IBaseQuery whereIsNotNull(String name){
        addCondition(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_NOT_NULL)));
        return this;
    }

    public IBaseQuery whereIsEmpty(String name){
        addCondition(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_EMPTY)));
        return this;
    }

    public IBaseQuery whereIsNotEmpty(String name){
        addCondition(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_NOT_EMPTY)));
        return this;
    }

    @Override
    public <T,R> IBaseQuery where(LambdaQuery<T,R> fieldFunction, ExpressionType type){
        return where(fieldFunction.getColumnName(), type);
    }

    @Override
    public IBaseQuery where(String name, ExpressionType type){
        if(ExpressionType.CDT_IS_NULL == type
                || ExpressionType.CDT_IS_NOT_NULL == type
                || ExpressionType.CDT_IS_EMPTY == type
                || ExpressionType.CDT_IS_NOT_EMPTY == type
                ) {
            addCondition(new Condition().and(new Expression(new Column(name), type)));
        }
        return this;
    }

    @Override
    public <T,R> IBaseQuery whereEqual(LambdaQuery<T,R> fieldFunction, Object value){
        return whereEqual(fieldFunction.getColumnName(), value);
    }

    @Override
    public IBaseQuery whereEqual(String name, Object value) {
        return where(name, value, ExpressionType.CDT_EQUAL);
    }

    @Override
    public <T,R> IBaseQuery whereNotEqual(LambdaQuery<T,R> fieldFunction, Object value){
        return whereNotEqual(fieldFunction.getColumnName(), value);
    }
    @Override
    public IBaseQuery whereNotEqual(String name, Object value) {
        return where(name, value, ExpressionType.CDT_NOT_EQUAL);
    }

    @Override
    public IBaseQuery whereLike(String name, String value) {
        return where(name, value, ExpressionType.CDT_LIKE);
    }

    @Override
    public IBaseQuery whereNotLike(String name, String value) {
        return where(name, value, ExpressionType.CDT_NOT_LIKE);
    }

    @Override
    public IBaseQuery whereStartWith(String name, String value) {
        return where(name, value, ExpressionType.CDT_START_WITH);
    }

    @Override
    public IBaseQuery whereEndWith(String name, String value) {
        return where(name, value, ExpressionType.CDT_END_WITH);
    }

    @Override
    public IBaseQuery whereNotStartWith(String name, String value) {
        return where(name, value, ExpressionType.CDT_NOT_START_WITH);
    }

    @Override
    public IBaseQuery whereNotEndWith(String name, String value) {
        return where(name, value, ExpressionType.CDT_NOT_END_WITH);
    }

    @Override
    public <T,R> IBaseQuery whereLike(LambdaQuery<T,R> fieldFunction, String value){
        return whereLike(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereNotLike(LambdaQuery<T,R> fieldFunction, String value){
        return whereNotLike(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereStartWith(LambdaQuery<T,R> fieldFunction, String value){
        return whereStartWith(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereEndWith(LambdaQuery<T,R> fieldFunction, String value){
        return whereEndWith(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereNotStartWith(LambdaQuery<T,R> fieldFunction, String value){
        return whereNotStartWith(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereNotEndWith(LambdaQuery<T,R> fieldFunction, String value){
        return whereNotEndWith(fieldFunction.getColumnName(), value);
    }

    @Override
    public IBaseQuery whereLess(String name, Object value) {
        return where(name, value, ExpressionType.CDT_LESS);
    }

    @Override
    public IBaseQuery whereLessEqual(String name, Object value) {
        return where(name, value, ExpressionType.CDT_LESS_EQUAL);
    }

    @Override
    public IBaseQuery whereMore(String name, Object value) {
        return where(name, value, ExpressionType.CDT_MORE);
    }

    @Override
    public IBaseQuery whereMoreEqual(String name, Object value) {
        return where(name, value, ExpressionType.CDT_MORE_EQUAL);
    }

    @Override
    public <T,R> IBaseQuery whereLess(LambdaQuery<T,R> fieldFunction, Object value){
        return whereLess(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereLessEqual(LambdaQuery<T,R> fieldFunction, Object value){
        return whereLessEqual(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereMore(LambdaQuery<T,R> fieldFunction, Object value){
        return whereMore(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereMoreEqual(LambdaQuery<T,R> fieldFunction, Object value){
        return whereMoreEqual(fieldFunction.getColumnName(), value);
    }

    @Override
    public IBaseQuery whereIn(String name, Collection value) {
        return where(name, value, ExpressionType.CDT_IN);
    }

    @Override
    public IBaseQuery whereNotIn(String name, Collection value) {
        return where(name, value, ExpressionType.CDT_NOT_IN);
    }

    @Override
    public IBaseQuery whereInValues(String name, Object... values) {
        List<Object> temp = null;
        if(values != null){
            temp = Arrays.asList(values);
        }else{
            temp = new ArrayList<Object>();
        }
        return where(name, temp, ExpressionType.CDT_IN);
    }

    @Override
    public IBaseQuery whereNotInValues(String name, Object... values) {
        List<Object> temp = null;
        if(values != null){
            temp = Arrays.asList(values);
        }else{
            temp = new ArrayList<Object>();
        }
        return where(name, temp, ExpressionType.CDT_NOT_IN);
    }

    @Override
    public <T,R> IBaseQuery whereIn(LambdaQuery<T,R> fieldFunction, Collection value){
        return whereIn(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereNotIn(LambdaQuery<T,R> fieldFunction, Collection value){
        return whereNotIn(fieldFunction.getColumnName(), value);
    }
    @Override
    public <T,R> IBaseQuery whereInValues(LambdaQuery<T,R> fieldFunction, Object ... values){
        return whereInValues(fieldFunction.getColumnName(), values);
    }
    @Override
    public <T,R> IBaseQuery whereNotInValues(LambdaQuery<T,R> fieldFunction, Object ... values){
        return whereNotInValues(fieldFunction.getColumnName(), values);
    }
    @Override
    public IBaseQuery where(String name, Object value, ExpressionType type){
        addCondition(new Condition().and(new Expression(new Column(name), value, type)));
		return this;
	}

    @Override
    public <T,R> IBaseQuery whereIsNull(LambdaQuery<T,R> fieldFunction){
        return whereIsNull(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery whereIsNotNull(LambdaQuery<T,R> fieldFunction){
        return whereIsNotNull(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery whereIsEmpty(LambdaQuery<T,R> fieldFunction){
        return whereIsEmpty(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery whereIsNotEmpty(LambdaQuery<T,R> fieldFunction){
        return whereIsNotEmpty(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery where(LambdaQuery<T,R> fieldFunction, Object value, ExpressionType type){
        return where(fieldFunction.getColumnName(), value, type);
    }

	public IBaseQuery where(Condition condition) {
        if(condition != null) {
            addCondition(condition);
        }
		return this;
	}

    @Override
    public IBaseQuery whereBetween(String name, Object value, Object andValue){
        if(!ORMUtils.isEmpty(value) && !ORMUtils.isEmpty(andValue)){
            Expression expression = new Expression(new Column(name), value, ExpressionType.CDT_BETWEEN);
            expression.setAndValue(andValue);
            addCondition(new Condition().and(expression));
        }
        return this;
    }

    @Override
    public <T,R> IBaseQuery whereBetween(LambdaQuery<T,R> fieldFunction, Object value, Object andValue){
        return whereBetween(fieldFunction.getColumnName(), value, andValue);
    }

    @Override
    public IBaseQuery where(Expression... expressions) {
        addCondition(new Condition().and(expressions));
        return this;
    }

    public IBaseQuery group(String name) {
		groups.add(new Column(name));
		return this;
	}

    public List<Column> getGroupCountSelectColumns(){
        return groupCountsSelectColumns;
    }

    public IBaseQuery groupCountSelectColumn(String name) {
        groupCountsSelectColumns.add(new Column(name));
        return this;
    }

	@Override
	public IBaseQuery having(String name, Object value, ExpressionType type) {
        if(value == null){
            return this;
        }
        if("".equals(value)){
            return this;
        }
        addHaving(new Condition().or(new Expression(new Column(name), value, type)));

		return this;
	}

    @Override
    public <T,R> IBaseQuery group(LambdaQuery<T,R> fieldFunction){
        return group(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery groupCountSelectColumn(LambdaQuery<T,R> fieldFunction){
        return groupCountSelectColumn(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery having(LambdaQuery<T,R> fieldFunction, Object value, ExpressionType type){
        return having(fieldFunction.getColumnName(), value, type);
    }
    @Override
	public IBaseQuery having(Condition condition) {
        addCondition(condition);
		return this;
	}

    @Override
	public IBaseQuery orderAsc(String name){
		orders.add(new Order(new Column(name), Order.ASC));
		return this;
	}

    @Override
    public <T,R> IBaseQuery orderDesc(LambdaQuery<T,R> fieldFunction){
        return orderDesc(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery orderAsc(LambdaQuery<T,R> fieldFunction){
        return orderAsc(fieldFunction.getColumnName());
    }


    public IBaseQuery order(Order order) {
        if (order != null) {
            orders.add(order);
        }
        return this;
    }

    public IBaseQuery orders(List<Order> os){
        if(os != null){
            for(Order order : os){
                orders.add(order);
            }
        }
        return this;
    }

    @Override
    public <T,R> IBaseQuery createQuery(LambdaQuery<T,R> ... lambdaQueries){
        return createQuery(QueryUtils.getColumns(lambdaQueries));
    }
    @Override
    public <T,R> IBaseQuery createQuery(Class<?> clazz, LambdaQuery<T,R> ... names){
        return createQuery(clazz, QueryUtils.getColumns(names));
    }

    @Override
    public IBaseQuery createQuery(Class<?> clazz, String... names){
        this.returnClass = clazz;
        this.returnColumns.clear();
        this.fixedReturnColumns.clear();
        if(names != null){
            for(String name : names){
                returnColumns.add(new Column(name));
            }
        }
        return this;
    }

    @Override
    public IBaseQuery createQuery(Class<?> clazz, Column... columns){
        this.returnClass = clazz;
        this.returnColumns.clear();
        this.fixedReturnColumns.clear();
        if(columns != null){
            for(Column column : columns){
                returnColumns.add(column);
            }
        }
        return this;
    }

    @Override
    public IBaseQuery addReturnColumn(Column column){
        if(column != null) {
            this.returnColumns.add(column);
        }
        return this;
    }

    @Override
    public IBaseQuery addReturnColumn(String column){
        if(ORMUtils.isEmpty(column)) {
            this.returnColumns.add(new Column(column));
        }
        return this;
    }

    @Override
    public IBaseQuery clearReturnColumns(){
        this.returnColumns.clear();
        return this;
    }

    @Override
    public IBaseQuery addFixedReturnColumn(Column column){
        if(column != null) {
            this.fixedReturnColumns.add(column);
        }
        return this;
    }

    @Override
    public <T,R> IBaseQuery addReturnColumn(LambdaQuery<T,R> fieldFunction){
        return addReturnColumn(fieldFunction.getColumnName());
    }
    @Override
    public <T,R> IBaseQuery addFixedReturnColumn(LambdaQuery<T,R> fieldFunction){
        return addFixedReturnColumn(fieldFunction.getColumnName());
    }

    @Override
    public IBaseQuery addFixedReturnColumn(String column){
        if(ORMUtils.isEmpty(column)) {
            this.fixedReturnColumns.add(new Column(column));
        }
        return this;
    }

    @Override
    public IBaseQuery clearFixedReturnColumns(){
        this.returnColumns.clear();
        return this;
    }


    @Override
    public IBaseQuery createQuery(String... names){
        this.returnClass = this.table;
        this.returnColumns.clear();
        this.fixedReturnColumns.clear();
        if(names != null){
            for(String name : names){
                returnColumns.add(new Column(name));
            }
        }
        return this;
    }

    public IBaseQuery createQuery(Column... columns) {
        this.returnClass = this.table;
        this.returnColumns.clear();
        this.fixedReturnColumns.clear();
        if(columns != null){
           for(Column column : columns){
               returnColumns.add(column);
           }
        }
        return this;
    }

    @Override
    public IBaseQuery distinct() {
        this.distinct = true;
        return this;
    }


    public IBaseQuery table(Class<?> clazz){
        if(clazz != null) {
            RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
            if(rdTable != null) {
                this.table = clazz;
                this.returnClass = clazz;
            }
        }
		return this;
	}


    @Override
    public Class<?> getTable() {
        return table;
    }

    @Override
    public List<Column> getGroups() {
        return groups;
    }

    @Override
    public Class<?> getReturnClass() {
        return returnClass;
    }

    @Override
    public List<Column> getReturnColumns() {
        List<Column> columnList = new ArrayList<Column>();
        if(returnColumns.isEmpty()){
            columnList.add(new Column(Column.ALL));
        }else{
            columnList.addAll(returnColumns);
        }
        return columnList;
    }

    @Override
    public List<Column> getFinalReturnColumns() {
        List<Column> columnList = new ArrayList<Column>();
        columnList.addAll(getReturnColumns());
        columnList.addAll(fixedReturnColumns);
        return columnList;
    }

    @Override
    public List<Column> getFixedReturnColumns() {
        return fixedReturnColumns;
    }

    public QueryInfo doQuery() {
        return options.doQuery(this, getPageable());
    }

    public QueryInfo doQueryCount() {
        return options.doQueryCount(this);
    }

}
