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
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.support.*;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.*;

public class MultiQueryImpl implements IMultiQuery {

    private List<String> aliasList = new ArrayList<String>();
	private Map<String, Class<?>> aliasTable = new HashMap<String, Class<?>>();
    private Map<String, IQuery> aliasQuery = new HashMap<String, IQuery>();
    private List<Condition> conditions = new ArrayList<Condition>();
	private List<Condition> havings = new ArrayList<Condition>();
	private List<Column> groups = new ArrayList<Column>();
	private List<Order> orders = new ArrayList<Order>();
	private List<Join> joins = new ArrayList<Join>();

    private Class<?> returnClass;
    private List<Column> returnColumns = new ArrayList<Column>();

	private boolean distinct = false;
	
	public IMultiQuery createQuery(Class<?> clazz, Column... columns) {
		this.returnClass = clazz;
        if(columns != null) {
            for (Column column : columns) {
                returnColumns.add(column);
            }
        }
		return this;
	}

    public IMultiQuery createQuery(Column... columns) {
        if(columns != null) {
            for (Column column : columns) {
                returnColumns.add(column);
            }
        }
        return this;
    }

    public IMultiQuery createQuery(Class<?> clazz, Columns... columns){
        this.returnClass = clazz;
        if(columns != null) {
            for (Columns column : columns) {
                if(!column.getColumnList().isEmpty()) {
                    returnColumns.addAll(column.getColumnList());
                }
            }
        }
        return this;
    }

    /////////////////////////////////
    private String generateUniqueAlias(Object object){
        int i = 0;
        String prefixAlias = null;
        if(object instanceof IMultiQuery || object instanceof IQuery){
            prefixAlias = "q";
        }else if(object instanceof Class){
            prefixAlias = ((Class<?>)object).getSimpleName().substring(0, 1).toLowerCase();
        }else{
            prefixAlias = "unknown";
        }
        String alias = prefixAlias + i;
        while(containsAlias(alias)){
            i++;
            alias = prefixAlias + i;
        }
        return alias;
    }

    public AliasTable join(IQuery query){
        AliasTable table = null;
        if(query != null) {
            table = new AliasTable(query);
            String alias = generateUniqueAlias(query);
            table.setAlias(alias);
        }
        return table;
    }

    public AliasTable table(IQuery query){
        AliasTable table = null;
        if(query != null) {
            table = new AliasTable(query);
            String alias = generateUniqueAlias(query);
            table.setAlias(alias);
            aliasList.add(alias);
            aliasQuery.put(alias, query);
        }
        return table;
    }

    public AliasTable join(Class<?> clazz){
        AliasTable table = null;
        if(clazz != null) {
            RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
            if(rdTable != null) {
                table = new AliasTable(clazz);
                String alias = generateUniqueAlias(clazz);
                table.setAlias(alias);
            }
        }
        return table;
    }

    @Override
    public IMultiQuery whereEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_Equal);
    }

    @Override
    public IMultiQuery whereNotEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_NotEqual);
    }

    @Override
    public IMultiQuery whereLike(Column left, String value) {
        return where(left, value, ExpressionType.CDT_Like);
    }

    @Override
    public IMultiQuery whereNotLike(Column left, String value) {
        return where(left, value, ExpressionType.CDT_NotLike);
    }

    @Override
    public IMultiQuery whereStartWith(Column left, String value) {
        return where(left, value, ExpressionType.CDT_StartWith);
    }

    @Override
    public IMultiQuery whereEndWith(Column left, String value) {
        return where(left, value, ExpressionType.CDT_EndWith);
    }

    @Override
    public IMultiQuery whereLess(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_Less);
    }

    @Override
    public IMultiQuery whereLessEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_LessEqual);
    }

    @Override
    public IMultiQuery whereMore(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_More);
    }

    @Override
    public IMultiQuery whereMoreEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_MoreEqual);
    }


    public AliasTable table(Class<?> clazz){
        if(this.returnClass == null){
            this.returnClass = clazz;
        }
        AliasTable table = null;
        if(clazz != null) {
            RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
            if(rdTable != null) {
                table = new AliasTable(clazz);
                String alias = generateUniqueAlias(clazz);
                table.setAlias(alias);
                aliasList.add(alias);
                aliasTable.put(alias, clazz);
            }
        }
        return table;
    }

    public IMultiQuery where(Column left, ExpressionType type) {
        if(ExpressionType.CDT_IS_NULL == type || ExpressionType.CDT_IS_NOT_NULL == type) {
            conditions.add(new Condition().and(new Expression(left, type)));
        }
        return this;
    }

    public IMultiQuery whereIsNull(Column left) {
        conditions.add(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NULL)));
        return this;
    }

    public IMultiQuery whereIsNotNull(Column left) {
        conditions.add(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NOT_NULL)));
        return this;
    }

    public IMultiQuery where(Column left, Object value, ExpressionType type) {
        if(value == null){
            return this;
        }
        if("".equals(value)){
            return this;
        }
        conditions.add(new Condition().and(new Expression(left, value, type)));

        return this;
    }

    public IMultiQuery where(Column left, Column value) {
        conditions.add(new Condition().and(new Expression(left, value)));
        return this;
    }

    public IMultiQuery where(Column left, Column value, ExpressionType type) {
        conditions.add(new Condition().and(new Expression(left, value, type)));
        return this;
    }

    public IMultiQuery where(Condition condition) {
        if(condition != null) {
            conditions.add(condition);
        }
        return this;
    }

    @Override
    public IMultiQuery where(Expression... expressions) {
        conditions.add(new Condition().and(expressions));
        return this;
    }


    public IMultiQuery group(Column ... columns) {
        if(columns != null) {
            groups.addAll(Arrays.asList(columns));
        }
        return this;
    }

    public IMultiQuery group(Columns ... columns) {
        if(columns != null) {
            for(Columns column : columns) {
                groups.addAll(column.getColumnList());
            }
        }
        return this;
    }

    public IMultiQuery having(Column left, Object value, ExpressionType type) {
        if(value == null){
            return this;
        }
        if("".equals(value)){
            return this;
        }
        havings.add(new Condition().or(new Expression(left, value, type)));
        return this;
    }

    public IMultiQuery having(Column left, Column value) {
        havings.add(new Condition().and(new Expression(left, value)));
        return this;
    }

    public IMultiQuery having(Condition condition) {
        if(condition != null) {
            havings.add(condition);
        }
        return this;
    }



    public IMultiQuery orderDesc(Column column) {
        orders.add(new Order(column, Order.DESC));
        return this;
    }

    public IMultiQuery orderAsc(Column column) {
        orders.add(new Order(column, Order.ASC));
        return this;
    }

    public IMultiQuery orders(List<Order> orderList){
        if(orderList != null){
            for(Order order : orderList){
                orders.add(order);
            }
        }
        return this;
    }

    public IMultiQuery join(Join join){
        joins.add(join);
        return this;
    }

    @Override
    public IMultiQuery distinct() {
        this.distinct = true;
        return this;
    }


    @Override
    public Map<String, IQuery> getAliasQuery() {
        return aliasQuery;
    }

    @Override
    public Class<?> getTable() {
        return null;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public List<String> getAliasList() {
        return aliasList;
    }

    @Override
    public Map<String, Class<?>> getAliasTable() {
        return aliasTable;
    }

    @Override
    public List<Join> getJoins() {
        return joins;
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
        return returnColumns;
    }

    public boolean containsAlias(String alias){
        boolean result = aliasList.contains(alias);
        if(!result){
            for(Join join : joins){
                result = alias.equals(join.getAlias());
                if(result){
                    break;
                }
            }
        }
        if(!result){
            for(IQuery query : aliasQuery.values()){
                if(query instanceof IMultiQuery){
                    result = ((IMultiQuery)query).containsAlias(alias);
                    if(result){
                        break;
                    }
                }
            }
        }
        return result;
    }

}
