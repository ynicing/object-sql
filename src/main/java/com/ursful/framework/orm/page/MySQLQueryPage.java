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
import java.util.concurrent.atomic.AtomicInteger;

public class MySQLQueryPage extends AbstractQueryPage{

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
            if(!ORMUtils.join(temp, "").contains(Expression.EXPRESSION_ALL)) {
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

        sb.append(getWordAfterFrom(query, values, false, baseName));

        if(page != null){
            sb.append(" LIMIT ? OFFSET ? ");
            values.add(new Pair(new Integer(page.getSize())));
            values.add(new Pair(new Integer(page.getOffset())));
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
        sql.append(" FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(terms != null) {
            String conditions = QueryUtils.getConditions(ORMUtils.newList(terms.getCondition()), values);
            if (conditions != null && !"".equals(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }

        if(multiOrder != null) {
            String orders = QueryUtils.getOrders(multiOrder.getOrders());
            if (orders != null && !"".equals(orders)) {
                sql.append(" ORDER BY " + orders);
            }
        }

        if(start != null && size != null){
            sql.append(" LIMIT ? OFFSET ? ");
            values.add(new Pair(size));
            values.add(new Pair(start));
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;
    }
}
