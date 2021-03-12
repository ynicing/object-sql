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
import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.*;

public class MultiQueryImpl implements IMultiQuery {

    private List<IMultiQuery> subQueries = new ArrayList<IMultiQuery>();

    private List<String> usedAlias = new ArrayList<String>();

    public  MultiQueryImpl(){}

    private IMultiQuery parentQuery;

    public MultiQueryImpl(IMultiQuery query){
        this.parentQuery = query;
    }

    private List<String> aliasList = new ArrayList<String>();
    private List<String> aliasJoin = new ArrayList<String>();
	private Map<String, Object> aliasTable = new HashMap<String, Object>();
//    private Map<String, IQuery> aliasQuery = new HashMap<String, IQuery>();
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
        if(object instanceof IQuery){
            prefixAlias = "q";
        }else if(object instanceof Class){
            prefixAlias = ((Class<?>)object).getSimpleName().substring(0, 1).toLowerCase(Locale.ROOT);
        }else{
            prefixAlias = object.toString().substring(0, 1).toLowerCase(Locale.ROOT);
        }
        String alias = prefixAlias + i;
        while(containsAlias(alias)){
            i++;
            alias = prefixAlias + i;
        }
        return alias;
    }

    public AliasTable join(IQuery query){
        String alias = generateUniqueAlias(query);
        return join(query, alias);
    }

    public AliasTable join(IQuery query, String alias){
        AliasTable table = new AliasTable(query);
        table.setAlias(alias);
        aliasJoin.add(alias);
        addUsedAlias(alias);
        return table;
    }

    public AliasTable table(IQuery query, String alias){
        AliasTable table = new AliasTable(query);
        table.setAlias(alias);
        aliasList.add(alias);
        addUsedAlias(alias);
        aliasTable.put(alias, query);
        return table;
    }

    public AliasTable table(IQuery query){
        String alias = generateUniqueAlias(query);
        return table(query, alias);
    }

    public AliasTable join(Class<?> clazz, String alias){
        ORMUtils.whenEmpty(clazz, "AliasTable join class should not be null.");
        RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
        ORMUtils.whenEmpty(rdTable, "Should annotate RdTable");
        AliasTable table = new AliasTable(clazz);
        table.setAlias(alias);
        aliasJoin.add(alias);
        addUsedAlias(alias);
        return table;
    }

    public AliasTable join(Class<?> clazz){
        String alias = generateUniqueAlias(clazz);
        return join(clazz, alias);
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


    @Override
    public IMultiQuery whereIn(Column left, Collection value) {
        return where(left, value, ExpressionType.CDT_In);
    }


    public IMultiQuery whereInValues(Column left, Object ... values){
        List<Object> temp = null;
        if(values != null){
            temp = Arrays.asList(values);
        }else{
            temp = new ArrayList<Object>();
        }
        return where(left, temp, ExpressionType.CDT_In);
    }
    public IMultiQuery whereNotInValues(Column left, Object ... values){
        List<Object> temp = null;
        if(values != null){
            temp = Arrays.asList(values);
        }else{
            temp = new ArrayList<Object>();
        }
        return where(left, temp, ExpressionType.CDT_NotIn);
    }

    @Override
    public IMultiQuery whereNotIn(Column left, Collection value) {
        return where(left, value, ExpressionType.CDT_NotIn);
    }

    @Override
    public IMultiQuery whereNotInQuery(Column left, IMultiQuery query) {
        return where(left, query, ExpressionType.CDT_NotIn);
    }

    @Override
    public IMultiQuery whereInQuery(Column left, IMultiQuery query) {

        return where(left, query, ExpressionType.CDT_In);
    }

    public AliasTable table(Class<?> clazz){
        String alias = generateUniqueAlias(clazz);
        return table(clazz, alias);
    }

    @Override
    public AliasTable table(String tableName) {
        String alias = generateUniqueAlias(tableName);
        AliasTable table = new AliasTable(tableName);
        table.setAlias(alias);
        aliasList.add(alias);
        aliasTable.put(alias, tableName);
        addUsedAlias(alias);
        return table;
    }

    public AliasTable table(Class<?> clazz, String alias){
        if(this.returnClass == null){
            this.returnClass = clazz;
        }
        AliasTable table = null;
        if(clazz != null) {
            RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
            if(rdTable != null) {
                table = new AliasTable(clazz);
                table.setAlias(alias);
                aliasList.add(alias);
                addUsedAlias(alias);
                aliasTable.put(alias, clazz);
            }
        }
        return table;
    }

//    public IMultiQuery where(Column left, ExpressionType type) {
//        if(ExpressionType.CDT_IS_NULL == type || ExpressionType.CDT_IS_NOT_NULL == type) {
//            conditions.add(new Condition().and(new Expression(left, type)));
//        }
//        return this;
//    }

    public IMultiQuery whereIsNull(Column left) {
        conditions.add(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NULL)));
        return this;
    }

    public IMultiQuery whereIsNotNull(Column left) {
        conditions.add(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NOT_NULL)));
        return this;
    }

    public IMultiQuery whereIsEmpty(Column left) {
        conditions.add(new Condition().and(new Expression(left, ExpressionType.CDT_IS_EMPTY)));
        return this;
    }

    public IMultiQuery whereIsNotEmpty(Column left) {
        conditions.add(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NOT_EMPTY)));
        return this;
    }

    public IMultiQuery where(Column left, Object value, ExpressionType type) {
        if(ORMUtils.isEmpty(value)){
            if(type == ExpressionType.CDT_IS_NULL
                    || type == ExpressionType.CDT_IS_NOT_NULL
                    || type == ExpressionType.CDT_IS_EMPTY
                    || type == ExpressionType.CDT_IS_NOT_EMPTY){
                conditions.add(new Condition().and(new Expression(left, type)));
            }
            return this;
        }
        conditions.add(new Condition().and(new Expression(left, value, type)));
        return this;
    }

    public IMultiQuery where(Column left, Column value) {
        conditions.add(new Condition().and(new Expression(left, value)));
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
        if(ORMUtils.isEmpty(value)){
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


//    @Override
//    public Map<String, IQuery> getAliasQuery() {
//        return aliasQuery;
//    }

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
    public Map<String, Object> getAliasTable() {
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
        if(returnColumns.isEmpty()){
            for(String alias : aliasList){
                returnColumns.add(new Column(alias, Expression.EXPRESSION_ALL));
            }
        }
        return returnColumns;
    }

    public void addUsedAlias(String alias){
        if(parentQuery == null) {
            usedAlias.add(alias);
        }else{
            IMultiQuery temp = parentQuery;
            while (temp != null){
                if(temp.parentQuery() == null){
                    temp.addUsedAlias(alias);
                }
                temp = temp.parentQuery();
            }
        }
    }

    public boolean containsAlias(String alias){
        if(parentQuery == null){
            return usedAlias.contains(alias);
        }
        IMultiQuery temp = parentQuery;
        boolean hasC = false;
        while (temp != null){
            if(temp.parentQuery() == null){
                hasC = temp.containsAlias(alias);
                break;
            }
            temp = temp.parentQuery();
        }
        return hasC;
    }

    @Override
    public IMultiQuery createMultiQuery() {
        IMultiQuery query = new MultiQueryImpl(this);
        subQueries.add(query);
        return query;
    }

    @Override
    public IMultiQuery parentQuery() {
        return this.parentQuery;
    }

    private  Options options;

    @Override
    public void setOptions(Options options) {
        this.options = options;
        for (IMultiQuery query : this.subQueries){
            query.setOptions(options);
        }
    }

    @Override
    public Options getOptions() {
        return this.options;
    }

    @Override
    public QueryInfo doQuery() {
        if(parentQuery != null) {
            return parentQuery.getOptions().doQuery(this, getPageable());
        }else{
            return options.doQuery(this, getPageable());
        }
    }

    @Override
    public QueryInfo doQueryCount() {
        if(parentQuery != null) {
            return parentQuery.getOptions().doQueryCount(this);
        }else{
            return options.doQueryCount(this);
        }
    }

    @Override
    public IMultiQuery whereExists(IMultiQuery query) {
        conditions.add(new Condition().and(new Expression(ExpressionType.CDT_EXISTS, query)));
        return this;
    }

    @Override
    public IMultiQuery whereNotExists(IMultiQuery query) {
        conditions.add(new Condition().and(new Expression(ExpressionType.CDT_NOT_EXISTS, query)));
        return this;
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

}
