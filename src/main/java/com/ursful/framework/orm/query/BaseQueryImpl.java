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
package com.ursful.framework.orm.query;

import com.ursful.framework.orm.IMultiQuery;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.IBaseQuery;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.*;

public class BaseQueryImpl implements IBaseQuery {

    private String id;
	
    private Class<?> table;
    private Class<?> returnClass;
    private List<Column> returnColumns = new ArrayList<Column>();

	private List<Condition> conditions = new ArrayList<Condition>();
	private List<Condition> havings = new ArrayList<Condition>();
    private List<Column> groups = new ArrayList<Column>();
    private List<Column> groupCountsSelectColumns = new ArrayList<Column>();
	private List<Order> orders = new ArrayList<Order>();
	
	private boolean distinct = false;
	
	public IBaseQuery orderDesc(String name){
		orders.add(new Order(new Column(name), Order.DESC));
		return this;
	}

    public IBaseQuery whereIsNull(String name){
        conditions.add(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_NULL)));
        return this;
    }

    public IBaseQuery whereIsNotNull(String name){
        conditions.add(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_NOT_NULL)));
        return this;
    }

    public IBaseQuery whereIsEmpty(String name){
        conditions.add(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_EMPTY)));
        return this;
    }

    public IBaseQuery whereIsNotEmpty(String name){
        conditions.add(new Condition().and(new Expression(new Column(name), ExpressionType.CDT_IS_NOT_EMPTY)));
        return this;
    }

    public IBaseQuery where(String name, ExpressionType type){
        if(ExpressionType.CDT_IS_NULL == type
                || ExpressionType.CDT_IS_NOT_NULL == type
                || ExpressionType.CDT_IS_EMPTY == type
                || ExpressionType.CDT_IS_NOT_EMPTY == type
                ) {
            conditions.add(new Condition().and(new Expression(new Column(name), type)));
        }
        return this;
    }

    @Override
    public IBaseQuery whereEqual(String name, Object value) {
        return where(name, value, ExpressionType.CDT_EQUAL);
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

    public IBaseQuery where(String name, Object value, ExpressionType type){
        conditions.add(new Condition().and(new Expression(new Column(name), value, type)));
		return this;
	}

	public IBaseQuery where(Terms terms) {
        if(terms != null) {
            conditions.add(terms.getCondition());
        }
		return this;
	}

    @Override
    public IBaseQuery where(Express... expresses) {
        if(expresses != null){
            for(Express express : expresses) {
                if(express == null){
                    continue;
                }
                conditions.add(new Condition().and(express.getExpression()));
            }
        }
        return this;
    }

    @Override
    public IBaseQuery whereBetween(String name, Object value, Object andValue){
        if(!ORMUtils.isEmpty(value) && !ORMUtils.isEmpty(andValue)){
            Expression expression = new Expression(new Column(name), value, ExpressionType.CDT_BETWEEN);
            expression.setAndValue(andValue);
            conditions.add(new Condition().and(expression));
        }
        return this;
    }

    @Override
    public IBaseQuery where(Expression... expressions) {
        conditions.add(new Condition().and(expressions));
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

	
	public IBaseQuery having(String name, Object value, ExpressionType type) {
        if(value == null){
            return this;
        }
        if("".equals(value)){
            return this;
        }
        havings.add(new Condition().or(new Expression(new Column(name), value, type)));

		return this;
	}

	public IBaseQuery having(Terms terms) {
		conditions.add(terms.getCondition());
		return this;
	}

	public IBaseQuery orderAsc(String name){
		orders.add(new Order(new Column(name), Order.ASC));
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
    public IBaseQuery createQuery(Class<?> clazz, String... names){
        this.returnClass = clazz;
        this.returnColumns.clear();
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
    public IBaseQuery createQuery(String... names){
        this.returnClass = this.table;
        this.returnColumns.clear();
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

//    @Override
//    public Map<String, IQuery> getAliasQuery() {
//        return null;
//    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void setId(String id){
        this.id = id;
    }

    @Override
    public Class<?> getTable() {
        return table;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public List<Condition> getHavings() {
        return havings;
    }

    @Override
    public List<Column> getGroups() {
        return groups;
    }

    @Override
    public List<Order> getOrders() {
        return orders;
    }

    @Override
    public Class<?> getReturnClass() {
        return returnClass;
    }

    @Override
    public List<Column> getReturnColumns() {
        if(returnColumns.isEmpty()){
            returnColumns.add(new Column(Expression.EXPRESSION_ALL));
        }
        return returnColumns;
    }

    private Pageable pageable;

    @Override
    public Pageable getPageable() {
        return this.pageable;
    }

    @Override
    public void setPageable(Pageable pageable) {
        this.pageable = pageable;
    }

    private Options options;

    @Override
    public void setOptions(Options options) {
        this.options = options;
    }

    @Override
    public Options getOptions() {
        return this.options;
    }

    public QueryInfo doQuery() {
        return options.doQuery(this, getPageable());
    }

    public QueryInfo doQueryCount() {
        return options.doQueryCount(this);
    }
}
