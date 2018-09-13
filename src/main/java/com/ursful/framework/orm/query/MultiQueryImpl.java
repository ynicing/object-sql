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
	
	
//	private QueryInfo doQuery(Class<?> clazz, Column [] columns, Page page) throws CommonException {
//
//		QueryInfo qinfo = new QueryInfo();
//
//		List<Pair> values = new ArrayList<Pair>();
//
//		StringBuffer sb = new StringBuffer();
//        List<String> temp = new ArrayList<String>();
//        sb.append("SELECT ");
//		if(columns.length > 0){
//            if(isDistinct){
//                sb.append(" DISTINCT ");
//            }
//			for(Column column : columns){
//                temp.add(QueryUtils.parseColumn(column));
//			}
//            sb.append(ListUtils.join(temp, ","));
//		}else {
//            sb.append(" * ");
//        }
//
//		if(page != null && ConnectionManager.getManager().getDatabaseType() == DatabaseType.ORACLE && orders.isEmpty()){
//			sb.append(", ROWNUM rn_");
//		}
//
//		setFrom(sb, values);
//
//		if(page != null){
//			if(ConnectionManager.getManager().getDatabaseType() == DatabaseType.ORACLE){
//				if(orders.isEmpty()){
//					if(conditions.isEmpty()){
//						sb = new StringBuffer("SELECT * FROM (" + sb.toString() + " ROWNUM <= ? ) WHERE rn_ > ? ");
//					}else{
//						sb = new StringBuffer("SELECT * FROM (" + sb.toString() + " AND ROWNUM <= ? ) WHERE rn_ > ? ");
//					}
//				}else{
//					sb = new StringBuffer("SELECT * FROM (SELECT a_t_.*, ROWNUM rn_ FROM (" + sb.toString() + ") WHERE a_t_ ROWNUM <= ?) WHERE rn_ > ?  ");
//				}
//				values.add(new Pair(new Integer(page.getSize() + page.getOffset())));
//				values.add(new Pair(new Integer(page.getOffset())));
//			}else if(ConnectionManager.getManager().getDatabaseType() == DatabaseType.MYSQL){
//				sb.append(" LIMIT ? OFFSET ? ");
//				values.add(new Pair(new Integer(page.getSize())));
//				values.add(new Pair(new Integer(page.getOffset())));
//			}
//
//		}
//		qinfo.setClazz(clazz);
//		qinfo.setSql(sb.toString());
//		qinfo.setValues(values);
//		qinfo.setColumns(Arrays.asList(columns));
//		qinfo.setPage(page);
//
//		return qinfo;
//	}
	

    /*
	private QueryInfo doQuery(Column column) throws CommonException {
		
		QueryInfo qinfo = new QueryInfo();
		
		List<Pair> values = new ArrayList<Pair>();
		
		StringBuffer sb = new StringBuffer();
		String sn = column.getName();
		sb.append("SELECT " + (this.isDistinct?" distinct ":"") + sn);
		 
		setFrom(sb, values);
		qinfo.setClazz(String.class);
		qinfo.setSql(sb.toString());
		qinfo.setValues(values);
		qinfo.setColumn(column);
		
		return qinfo;
	}*/
	
	
	/*private QueryInfo doQueryCount(Column column) throws CommonException {
		
		QueryInfo qinfo = new QueryInfo();
		
		List<Pair> values = new ArrayList<Pair>();
		
		StringBuffer sb = new StringBuffer();
		if(column != null){
            sb.append("SELECT " + QueryUtils.parseColumn(column) +") ");
		}else{
            sb.append("SELECT " + Expression.EXPRESSION_COUNT + "("+ Expression.EXPRESSION_ALL +") ");
		}

		setFrom(sb, values);
		 
		qinfo.setSql(sb.toString());
		qinfo.setValues(values);
        if(columns != null) {
            qinfo.setColumns(Arrays.asList(columns));
        }
        qinfo.setClazz(Integer.class);
		qinfo.setPage(page);
		
		return qinfo;
	}*/


//	private void setFrom(StringBuffer sb, List<Pair> values) throws CommonException{
//
//		sb.append(" FROM ");
//        List<String> words = new ArrayList<String>();
//        for(String alias : aliasList) {
//            if(aliasTable.containsKey(alias)) {
//                words.add(aliasTable.get(alias) + " AS " + alias);
//            }else if(aliasQuery.containsKey(alias)){
//                IQuery query = aliasQuery.get(alias);
//                QueryInfo queryInfo = QueryUtils.doQuery(query.queryHelper(), null);
//                sb.append("(" + queryInfo.getSql() + ") AS " + alias);
//                values.addAll(queryInfo.getValues());
//            }
//        }
//        sb.append(ListUtils.join(words, ","));
//
//        String join = join(joins, values);
//        if(join != null && !"".equals(join)){
//            sb.append(join);
//        }
//
//		String whereCondition = QueryUtils.getConditions(conditions, values);
//		if(whereCondition != null){
//			sb.append(" WHERE " + whereCondition);
//		}
//
//		String groupString = QueryUtils.getGroups(groups);
//
//		if(groupString != null){
//			sb.append(" GROUP BY ");
//			sb.append(groupString);
//		}
//
//		String havingString = QueryUtils.getConditions(havings, values);
//		if(havingString != null){
//			sb.append(" HAVING ");
//			sb.append(havingString);
//		}
//
//        String orderString = QueryUtils.getOrders(orders);
//        if(orderString != null){
//            sb.append(" ORDER BY ");
//            sb.append(orderString);
//        }
//
//    }

//    public String join(List<Join> joins, List<Pair> values) throws CommonException{
//        StringBuffer sb = new StringBuffer();
//        for(int i = 0; i < joins.size(); i++){
//            Join join = joins.get(i);
//            String tableName = null;
//            if(join.getClazz() != null) {
//                RdTable rdTable = (RdTable)join.getClazz().getAnnotation(RdTable.class);
//                if(rdTable == null){
//                    continue;
//                }
//                tableName = rdTable.name();
//            }else if(join.getQuery() != null){
//                QueryInfo info = QueryUtils.doQuery(join.getQuery().queryHelper(), null);
//                tableName = "(" + info + ") ";
//            }
//
//            switch (join.getType()){
//                case FULL_JOIN:
//                    sb.append(" FULL JOIN ");
//                    break;
//                case INNER_JOIN:
//                    sb.append(" INNER JOIN ");
//                    break;
//                case LEFT_JOIN:
//                    sb.append(" LEFT JOIN ");
//                    break;
//                case RIGHT_JOIN:
//                    sb.append(" RIGHT JOIN ");
//                    break;
//            }
//            String alias = join.getAlias();
//
//            sb.append(tableName + " AS " + alias);
//
//            List<Condition> temp = join.getConditions();
//
//            String cdt = QueryUtils.getConditions(temp, values);
//            if(cdt != null && !"".equals(cdt)) {
//                sb.append(" ON ");
//                sb.append(cdt);
//            }
//        }
//
//        return sb.toString();
//    }


	
//	public IMultiQuery createCount() throws CommonException {
//		this.isDistinct = false;
//		this.qinfo = doQueryCount(null);
//		return this;
//	}

	
//	public IMultiQuery createCount(Column column) throws CommonException {
//		column.setFunction(Expression.EXPRESSION_COUNT);
//		this.isDistinct = false;
//		this.qinfo = doQueryCount(column);
//		return this;
//	}

	
//	public IMultiQuery createDistinctString(Column column) throws CommonException {
//		this.isDistinct = false;
//		this.qinfo = doQuery(column);
//		return this;
//	}

//	private Page page;
	
	
//	public IMultiQuery createPage(Page page) throws CommonException {
//		this.page = page;
//		this.isDistinct = false;
//		this.qinfo = doQuery(this.returnClass, this.columns, this.page);
//		return this;
//	}

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
	
//	public IMultiQuery createDistinctQuery(Class<?> clazz, Column... columns)
//			throws CommonException {
//		this.isDistinct = true;
//		this.returnClass = clazz;
//		this.columns = columns;
//		this.qinfo = doQuery(clazz, columns, null);
//		return this;
//	}


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
            RdTable rdTable = (RdTable)clazz.getAnnotation(RdTable.class);
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
            RdTable rdTable = (RdTable)clazz.getAnnotation(RdTable.class);
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
