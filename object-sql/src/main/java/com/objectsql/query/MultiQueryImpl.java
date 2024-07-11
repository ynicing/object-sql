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

import com.objectsql.IMultiQuery;
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;
import com.objectsql.IQuery;
import com.objectsql.annotation.RdTable;
import org.springframework.core.annotation.AnnotationUtils;

import java.util.*;

public class MultiQueryImpl extends QueryImpl implements IMultiQuery{

    private List<IMultiQuery> subQueries = new ArrayList<IMultiQuery>();

    private List<String> usedAlias = new ArrayList<String>();

    public  MultiQueryImpl(){}

    private IMultiQuery parentQuery;

    public MultiQueryImpl(IMultiQuery query){
        this.parentQuery = query;
    }

    private List<String> aliasList = new ArrayList<String>();
	private Map<String, Object> aliasTable = new HashMap<String, Object>();

	private List<Column> groups = new ArrayList<Column>();
    private List<Column> groupCountsSelectColumns = new ArrayList<Column>();

	private List<Join> joins = new ArrayList<Join>();

    private Class<?> returnClass;
    private List<Column> returnColumns = new ArrayList<Column>();
    private List<Column> fixReturnColumns = new ArrayList<Column>();

    private Map<Object, Column> dataColumns = new LinkedHashMap<Object, Column>();
    private Map<Object, String> dataKeys = new LinkedHashMap<Object, String>();

	public IMultiQuery createQuery(Class<?> clazz, Column... columns) {
		this.returnClass = clazz;
        this.returnColumns.clear();
        this.fixReturnColumns.clear();
        if(columns != null) {
            for (Column column : columns) {
                returnColumns.add(column);
            }
        }
		return this;
	}

    public IMultiQuery createQuery(Column... columns) {
        this.returnColumns.clear();
        this.fixReturnColumns.clear();
        if(columns != null) {
            for (Column column : columns) {
                returnColumns.add(column);
            }
        }
        return this;
    }

    public IMultiQuery createQuery(Class<?> clazz, Columns... columns){
        this.returnClass = clazz;
        this.returnColumns.clear();
        this.fixReturnColumns.clear();
        if(columns != null) {
            for (Columns column : columns) {
                List<Column> columnList = column.getColumnList();
                if(!columnList.isEmpty()) {
                    returnColumns.addAll(columnList);
                }
            }
        }
        return this;
    }

    public IMultiQuery addReturnColumn(Column column){
        if(column != null) {
            this.returnColumns.add(column);
        }
        return this;
    }

    public IMultiQuery clearReturnColumns(){
        this.returnColumns.clear();
        return this;
    }


    public IMultiQuery addFixedReturnColumn(Column column){
        if(column != null) {
            this.fixReturnColumns.add(column);
        }
        return this;
    }

    public IMultiQuery clearFixedReturnColumns(){
        this.fixReturnColumns.clear();
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
        aliasTable.put(alias, query);
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
        aliasTable.put(alias, clazz);
        addUsedAlias(alias);
        return table;
    }

    public AliasTable join(Class<?> clazz){
        String alias = generateUniqueAlias(clazz);
        return join(clazz, alias);
    }

    @Override
    public IMultiQuery whereEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_EQUAL);
    }

    @Override
    public IMultiQuery whereNotEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_NOT_EQUAL);
    }

    @Override
    public IMultiQuery whereLike(Column left, String value) {
        return where(left, value, ExpressionType.CDT_LIKE);
    }

    @Override
    public IMultiQuery whereNotLike(Column left, String value) {
        return where(left, value, ExpressionType.CDT_NOT_LIKE);
    }

    @Override
    public IMultiQuery whereStartWith(Column left, String value) {
        return where(left, value, ExpressionType.CDT_START_WITH);
    }

    @Override
    public IMultiQuery whereEndWith(Column left, String value) {
        return where(left, value, ExpressionType.CDT_END_WITH);
    }

    @Override
    public IMultiQuery whereNotStartWith(Column left, String value) {
        return where(left, value, ExpressionType.CDT_NOT_START_WITH);
    }

    @Override
    public IMultiQuery whereNotEndWith(Column left, String value) {
        return where(left, value, ExpressionType.CDT_NOT_END_WITH);
    }

    @Override
    public IMultiQuery whereLess(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_LESS);
    }

    @Override
    public IMultiQuery whereLessEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_LESS_EQUAL);
    }

    @Override
    public IMultiQuery whereMore(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_MORE);
    }

    @Override
    public IMultiQuery whereMoreEqual(Column left, Object value) {
        return where(left, value, ExpressionType.CDT_MORE_EQUAL);
    }


    @Override
    public IMultiQuery whereIn(Column left, Collection value) {
        return where(left, value, ExpressionType.CDT_IN);
    }

    @Override
    public IMultiQuery whereInValues(Column left, Object ... values){
        List<Object> temp = null;
        if(values != null){
            temp = Arrays.asList(values);
        }else{
            temp = new ArrayList<Object>();
        }
        return where(left, temp, ExpressionType.CDT_IN);
    }

    @Override
    public IMultiQuery whereNotInValues(Column left, Object ... values){
        List<Object> temp = null;
        if(values != null){
            temp = Arrays.asList(values);
        }else{
            temp = new ArrayList<Object>();
        }
        return where(left, temp, ExpressionType.CDT_NOT_IN);
    }

    @Override
    public IMultiQuery whereNotIn(Column left, Collection value) {
        return where(left, value, ExpressionType.CDT_NOT_IN);
    }

    @Override
    public IMultiQuery whereNotInQuery(Column left, IMultiQuery query) {
        return where(left, query, ExpressionType.CDT_NOT_IN);
    }

    @Override
    public IMultiQuery whereBetween(Column left, Object value, Object andValue){
        if(!ORMUtils.isEmpty(value) && !ORMUtils.isEmpty(andValue)){
            Expression expression = new Expression(left, value, ExpressionType.CDT_BETWEEN);
            expression.setAndValue(andValue);
            addCondition(new Condition().and(expression));
        }
        return this;
    }

    @Override
    public IMultiQuery whereInQuery(Column left, IMultiQuery query) {

        return where(left, query, ExpressionType.CDT_IN);
    }

    public AliasTable table(Class<?> clazz){
        String alias = generateUniqueAlias(clazz);
        return table(clazz, alias);
    }

    @Override
    public AliasTable table(String tableName, String alias) {
        AliasTable table = new AliasTable(tableName);
        table.setAlias(alias);
        aliasList.add(alias);
        aliasTable.put(alias, tableName);
        addUsedAlias(alias);
        return table;
    }

    @Override
    public AliasTable table(String tableName) {
        String alias = generateUniqueAlias(tableName);
        return table(tableName, alias);
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

    public IMultiQuery whereIsNull(Column left) {
        addCondition(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NULL)));
        return this;
    }

    public IMultiQuery whereIsNotNull(Column left) {
        addCondition(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NOT_NULL)));
        return this;
    }

    public IMultiQuery whereIsEmpty(Column left) {
        addCondition(new Condition().and(new Expression(left, ExpressionType.CDT_IS_EMPTY)));
        return this;
    }

    public IMultiQuery whereIsNotEmpty(Column left) {
        addCondition(new Condition().and(new Expression(left, ExpressionType.CDT_IS_NOT_EMPTY)));
        return this;
    }

    public IMultiQuery where(Column left, Object value, ExpressionType type) {
        if(ORMUtils.isEmpty(value)){
            if(type == ExpressionType.CDT_IS_NULL
                    || type == ExpressionType.CDT_IS_NOT_NULL
                    || type == ExpressionType.CDT_IS_EMPTY
                    || type == ExpressionType.CDT_IS_NOT_EMPTY){
                addCondition(new Condition().and(new Expression(left, type)));
            }
            return this;
        }
        addCondition(new Condition().and(new Expression(left, value, type)));
        return this;
    }

    public IMultiQuery where(Column left, Column value) {
        addCondition(new Condition().and(new Expression(left, value)));
        return this;
    }

    public IMultiQuery where(Condition condition) {
        if(condition != null) {
            addCondition(condition);
        }
        return this;
    }

    @Override
    public IMultiQuery where(Expression... expressions) {
        addCondition(new Condition().and(expressions));
        return this;
    }


    public IMultiQuery group(Column ... columns) {
        if(columns != null) {
            groups.addAll(Arrays.asList(columns));
        }
        return this;
    }

    public List<Column> getGroupCountSelectColumns(){
        return groupCountsSelectColumns;
    }

    public IMultiQuery groupCountSelectColumn(Column ... columns) {
        if(columns != null) {
            groupCountsSelectColumns.addAll(Arrays.asList(columns));
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
        addHaving(new Condition().or(new Expression(left, value, type)));
        return this;
    }

    public IMultiQuery having(Column left, Column value) {
        addHaving(new Condition().and(new Expression(left, value)));
        return this;
    }

    public IMultiQuery having(Condition condition) {
        if(condition != null) {
            addHaving(condition);
        }
        return this;
    }

    public IMultiQuery having(Expression expression) {
        if(expression != null) {
            addHaving(new Condition().and(expression));
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

    public IMultiQuery order(Order order) {
        if (order != null) {
            orders.add(order);
        }
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

    public IMultiQuery join(AliasTable table, Column left, Column right){
        return join(new Join(table).on(left, right));
    }

    public IMultiQuery join(AliasTable table, JoinType joinType, Column left, Column right){
        return join(new Join(table, joinType).on(left, right));
    }

    @Override
    public IMultiQuery distinct() {
        this.distinct = true;
        return this;
    }

    private boolean enableDataPermission = false;

    @Override
    public IMultiQuery enableDataPermission() {
        enableDataPermission = true;
        return this;
    }

    @Override
    public IMultiQuery disableDataPermission() {
        enableDataPermission = false;
        return this;
    }

    @Override
    public boolean isEnableDataPermission() {
        return enableDataPermission;
    }

    @Override
    public String dataKey(Object dataType) {
        return dataKeys.get(dataType);
    }

    @Override
    public IMultiQuery dataColumn(Object dataKey, Column column) {
        dataColumns.put(dataKey, column);
        return this;
    }

    @Override
    public IMultiQuery dataColumn(Object dataType, String dataKey, Column column) {
        dataColumns.put(dataType, column);
        dataKeys.put(dataType, dataKey);
        return this;
    }

    @Override
    public Column dataColumn(Object dataType) {
        return dataColumns.get(dataType);
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
            for(String alias : aliasList){
                columnList.add(new Column(alias, Column.ALL));
            }
        }else{
            columnList.addAll(returnColumns);
        }
        return columnList;
    }

    @Override
    public List<Column> getFinalReturnColumns() {
        List<Column> columnList = new ArrayList<Column>();
        columnList.addAll(getReturnColumns());
        columnList.addAll(fixReturnColumns);
        return columnList;
    }

    @Override
    public List<Column> getFixedReturnColumns() {
        return fixReturnColumns;
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
        query.setTextTransformType(this.textTransformType());
        if(this.isLessDatePlus235959()){
            query.enableLessDatePlus235959();
        }
        if (this.isLessEqualDatePlus235959()){
            query.enableLessEqualDatePlus235959();
        }
        subQueries.add(query);
        return query;
    }

    @Override
    public IMultiQuery parentQuery() {
        return this.parentQuery;
    }

    @Override
    public void setOptions(Options options) {
        this.options = options;
        for (IMultiQuery query : this.subQueries){
            query.setOptions(options);
        }
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
        addCondition(new Condition().and(new Expression(ExpressionType.CDT_EXISTS, query)));
        return this;
    }

    @Override
    public IMultiQuery whereNotExists(IMultiQuery query) {
        addCondition(new Condition().and(new Expression(ExpressionType.CDT_NOT_EXISTS, query)));
        return this;
    }


}
