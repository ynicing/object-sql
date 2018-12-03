package com.ursful.framework.orm.page;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractQueryPage implements QueryPage{

    @Override
    public QueryInfo doQueryCount(IQuery query) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        if(query.isDistinct()){
            sb.append(selectColumns(query, null));
        }else {
            sb.append(selectCount());
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        sb.append(joins(query, values));
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        info.setClazz(Integer.class);
        if(query.isDistinct()) {
            info.setSql("SELECT COUNT(*) FROM (" + sb.toString() + ")  _distinct_table");
        }else{
            info.setSql(sb.toString());
        }
        info.setValues(values);
        return info;
    }


    public String selectCount(){
        return " COUNT(*) ";
    }

    public String selectColumns(IQuery query, String alias){
        StringBuffer sb = new StringBuffer();
        if(query.isDistinct()) {
            sb.append(" DISTINCT ");
        }
        List<Column> returnColumns = query.getReturnColumns();
        List<String> temp = new ArrayList<String>();
        List<String> allAlias = new ArrayList<String>();
        List<String> inColumn = new ArrayList<String>();
        boolean noAlias = false;
        if(returnColumns.isEmpty()){
            String all = null;
            if(StringUtils.isEmpty(alias)){
                noAlias = true;
                all = Expression.EXPRESSION_ALL;
            }else{
                allAlias.add(alias);
                all = alias + "." + Expression.EXPRESSION_ALL;
            }
            temp.add(all);
        }else{
            for(Column column : returnColumns){
                inColumn.add(column.getAlias() + "." + column.getName());
                if(column.getAlias() == null && !StringUtils.isEmpty(alias)){
                    column.setAlias(alias);
                }
                temp.add(QueryUtils.parseColumn(column));
                if(Expression.EXPRESSION_ALL.equals(column.getName())){
                    if(!StringUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())){
                        allAlias.add(column.getAlias());
                    }else{
                        noAlias = true;
                    }
                }
            }
        }
        if(!noAlias) {
            List<Order> orders = query.getOrders();
            for (Order order : orders) {
                Column column = order.getColumn();
                QueryUtils.setColumnAlias(column, alias);
                if (!StringUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())
                        && !inColumn.contains(column.getAlias() + "." + column.getName())) {
                    String orderStr = QueryUtils.parseColumn(column);
                    temp.add(orderStr);
                }
            }
        }
        sb.append(ORMUtils.join(temp, ","));
        if(sb.length() == 0){
            if(StringUtils.isEmpty(alias)) {
                sb.append(Expression.EXPRESSION_ALL);
            }else{
                sb.append(alias + "." + Expression.EXPRESSION_ALL);
            }
        }
        return sb.toString();
    }

    public String orders(IQuery query, String alias){
        String result = "";
        List<Order> orders = query.getOrders();
        QueryUtils.setOrdersAlias(orders, alias);
        String orderString = QueryUtils.getOrders(orders);
        if (orderString != null && !"".equals(orderString)) {
            result = " ORDER BY " + orderString;
        }
        return result;
    }

    public String tables(IQuery query, List<Pair> values, String tableAlias){
        if(query.getTable() != null){
            String tableName = ORMUtils.getTableName(query.getTable());
            if(tableAlias  == null) {
                return tableName;
            }else{
                return tableName + " " + tableAlias;
            }
        }else{
            List<String> words = new ArrayList<String>();
            Map<String, Class<?>> aliasMap = query.getAliasTable();
            Map<String, IQuery> aliasQuery = query.getAliasQuery();
            List<String> aliasList = query.getAliasList();
            for(String alias : aliasList) {
                if(aliasMap.containsKey(alias)) {
                    String tn = ORMUtils.getTableName(aliasMap.get(alias));
                    words.add(tn + " " + alias);
                }else if(aliasQuery.containsKey(alias)){
                    IQuery q = aliasQuery.get(alias);
                    QueryInfo queryInfo = doQuery(q, null);
                    words.add("(" + queryInfo.getSql() + ") " + alias);
                    values.addAll(queryInfo.getValues());
                }
            }
            return ORMUtils.join(words, ",");
        }
    }

    public String wheres(IQuery query, List<Pair> values, String tableAlias){
        String result = "";
        List<Condition> conditions = query.getConditions();
        QueryUtils.setConditionsAlias(conditions, tableAlias);
        String whereCondition = QueryUtils.getConditions(query, conditions, values);
        if(whereCondition != null && !"".equals(whereCondition)){
            result =" WHERE " + whereCondition;
        }
        return result;
    }

    public String groups(IQuery query, String alias){
        String result = "";
        List<Column> columns = query.getGroups();
        QueryUtils.setColumnsAlias(columns, alias);
        String groupString = QueryUtils.getGroups(columns);
        if(groupString != null && !"".equals(groupString)){
            result =" GROUP BY " + groupString;
        }
        return result;
    }

    public String havings(IQuery query, List<Pair> values, String alias){
        String result = "";
        List<Condition> conditions = query.getHavings();
        QueryUtils.setConditionsAlias(conditions, alias);
        String havingString = QueryUtils.getConditions(query, conditions, values);
        if(havingString != null && !"".equals(havingString)){
            result = " HAVING " + havingString;
        }
        return result;
    }

    public String joins(IQuery obj, List<Pair> values){
        List<Join> joins = obj.getJoins();
        StringBuffer sb = new StringBuffer();
        if(joins == null){
            return  sb.toString();
        }
        for(int i = 0; i < joins.size(); i++){
            Join join = joins.get(i);
            String tableName = null;
            Object table = join.getTable();
            if(table instanceof Class) {
                tableName = ORMUtils.getTableName((Class)table);
            }else if(table instanceof IQuery){
                QueryInfo info = doQuery((IQuery)table, null);
                tableName = "(" + info.getSql() + ") ";
                values.addAll(info.getValues());
            }
            switch (join.getType()){
                case FULL_JOIN:
                    sb.append(" FULL JOIN ");
                    break;
                case INNER_JOIN:
                    sb.append(" INNER JOIN ");
                    break;
                case LEFT_JOIN:
                    sb.append(" LEFT JOIN ");
                    break;
                case RIGHT_JOIN:
                    sb.append(" RIGHT JOIN ");
                    break;
            }
            String alias = join.getAlias();

            sb.append(tableName + " " + alias);

            List<Condition> temp = join.getConditions();

            String cdt = QueryUtils.getConditions(obj, temp, values);
            if(cdt != null && !"".equals(cdt)) {
                sb.append(" ON ");
                sb.append(cdt);
            }
        }
        return sb.toString();
    }
}
