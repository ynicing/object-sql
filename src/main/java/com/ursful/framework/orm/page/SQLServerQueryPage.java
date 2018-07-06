package com.ursful.framework.orm.page;

import com.ursful.framework.core.exception.CommonException;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.error.ORMErrorCode;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018/7/5.
 */
public class SQLServerQueryPage extends AbstractQueryPage{

    private AtomicInteger count = new AtomicInteger(0);

    @Override
    public QueryInfo doQueryCount(IQuery query) {
        QueryInfo qinfo = new QueryInfo();

        List<Pair> values = new ArrayList<Pair>();

        StringBuffer sb = new StringBuffer();

        sb.append("SELECT ");
        if(!query.isDistinct()){
            sb.append("COUNT(*) ");
        }else{
            sb.append(" DISTINCT ");
            List<Column> returnColumns = query.getReturnColumns();
            List<String> temp = new ArrayList<String>();
            for(Column column : returnColumns){
                temp.add(QueryUtils.parseColumn(column));
            }
            sb.append(ORMUtils.join(temp, ","));
        }

        sb.append(getWordAfterFrom(query, values, true, null));

        qinfo.setClazz(Integer.class);
        if(query.isDistinct()) {
            qinfo.setSql("SELECT COUNT(*) FROM (" + sb.toString() + ")  _distinct_table");
        }else{
            qinfo.setSql(sb.toString());
        }
        qinfo.setValues(values);

        return qinfo;
    }

    @Override
    public QueryInfo doQuery(IQuery query, Page page) {
        QueryInfo qinfo = new QueryInfo();

        List<Pair> values = new ArrayList<Pair>();

        StringBuffer sb = new StringBuffer();
        List<String> temp = new ArrayList<String>();
        sb.append("SELECT ");
        String baseName = null;
        List<Column> returnColumns = query.getReturnColumns();
        if(returnColumns != null && returnColumns.size() > 0){
            if(query.isDistinct()){
                sb.append(" DISTINCT ");
            }

            for(Column column : returnColumns){
                temp.add(QueryUtils.parseColumn(column));
            }
            if(page == null && (!ORMUtils.join(temp, "").contains(Expression.EXPRESSION_ALL))) {
                List<Order> orders = query.getOrders();
                for (Order order : orders) {
                    String orderStr = QueryUtils.parseColumn(order.getColumn());
                    if (!temp.contains(orderStr)) {
                        temp.add(orderStr);
                    }
                }
            }
            sb.append(ORMUtils.join(temp, ","));
        }else {
            sb.append(" * ");
        }

        if(page != null){
//            select row_number() over(order by (select 0)) as rownumber,* from sys_user
            String byOrders = QueryUtils.getOrders(query.getOrders());
            if(byOrders != null){
                sb.append(" ,row_number() over(order by " + byOrders + ") rn_ ");
            }else{
                sb.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }

        sb.append(getWordAfterFrom(query, values, false, baseName, page));


        if(page != null){
            String tempSQL = sb.toString();
            sb = new StringBuffer("SELECT TOP " + page.getSize() +" * ");
            sb.append(" FROM (");
            sb.append(tempSQL);
            sb.append(") ms ");
            sb.append(" WHERE rn_ > " +page.getOffset()+" ");
//            values.add(0, new Pair(new Integer(page.getSize())));
           // values.add(new Pair(new Integer(page.getOffset())));

//            select distinct top 10 *
//                    from
//                            (
//                                    select row_number() over(order by (select 0)) as rownumber,* from sys_user
//            ) A
//            where rownumber > 0;

        }
        qinfo.setClazz(query.getReturnClass());
        qinfo.setSql(sb.toString());
        qinfo.setValues(values);
        qinfo.setColumns(query.getReturnColumns());

        return qinfo;
    }

    @Override
    public SQLHelper doQuery(Class<?> clazz, String[] names, Terms terms, MultiOrder multiOrder, Integer start, Integer size) {
        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("SELECT ");
        if(names != null && names.length == 2) {
            sql.append(names[0] + ", " + names[1]);
        }else{
            sql.append("*");
        }

        if(start != null && size != null){
            String byOrders = QueryUtils.getOrders(multiOrder.getOrders());
            if(byOrders != null){
                sql.append(" ,row_number() over(order by " + byOrders + ") rn_ ");
            }else{
                sql.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }

        sql.append(" FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(terms != null) {
            String conditions = QueryUtils.getConditions(ORMUtils.newList(terms.getCondition()), values);
            if (conditions != null && !"".equals(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }

        if(start != null && size != null){
            String tempSQL = sql.toString();
            sql = new StringBuffer("SELECT TOP " + size +" * ");
            sql.append(" FROM (");
            sql.append(tempSQL);
            sql.append(") ms ");
            sql.append(" WHERE rn_ > " + ((Math.max(1, start) - 1) * size)+" ");
        }else{
            if(multiOrder != null) {
                String orders = QueryUtils.getOrders(multiOrder.getOrders());
                if (orders != null && !"".equals(orders)) {
                    sql.append(" ORDER BY " + orders);
                }
            }
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;
    }
    public String getWordAfterFrom(IQuery query, List<Pair> values, boolean count, String baseName, Page page) throws CommonException{
        StringBuffer sb = new StringBuffer();
        sb.append(" FROM ");

        if(query.getTable() != null){
            RdTable table = (RdTable)query.getTable().getAnnotation(RdTable.class);
            sb.append(table.name());
            if(baseName != null){
                sb.append(" " +baseName);
            }
        }else{
            List<String> words = new ArrayList<String>();
            Map<String, Class<?>> aliasMap = query.getAliasTable();
            Map<String, IQuery> aliasQuery = query.getAliasQuery();
            List<String> aliasList = query.getAliasList();
            for(String alias : aliasList) {
                if(aliasMap.containsKey(alias)) {
                    RdTable table = (RdTable) aliasMap.get(alias).getAnnotation(RdTable.class);
                    words.add(table.name() + " " + alias);
                }else if(aliasQuery.containsKey(alias)){
                    IQuery q = aliasQuery.get(alias);
                    QueryInfo queryInfo = doQuery(q, null);
                    words.add("(" + queryInfo.getSql() + ") " + alias);
                    values.addAll(queryInfo.getValues());
                }
            }
            sb.append(ORMUtils.join(words, ","));
        }

        String join = join(query.getJoins(), values);
        if(join != null && !"".equals(join)){
            sb.append(join);
        }

        String whereCondition = QueryUtils.getConditions(query.getConditions(), values);
        if(whereCondition != null && !"".equals(whereCondition)){
            sb.append(" WHERE " + whereCondition);
        }

        if(page  == null){

            String groupString = QueryUtils.getGroups(query.getGroups());

            if(groupString != null && !"".equals(groupString)){
                sb.append(" GROUP BY ");
                sb.append(groupString);
            }

            String havingString = QueryUtils.getConditions(query.getHavings(), values);
            if(havingString != null && !"".equals(havingString)){
                sb.append(" HAVING ");
                sb.append(havingString);
            }

            if(!count) {
                String orderString = QueryUtils.getOrders(query.getOrders());
                if (orderString != null && !"".equals(orderString)) {
                    sb.append(" ORDER BY ");
                    sb.append(orderString);
                }
            }
        }

        return sb.toString();

    }

}
