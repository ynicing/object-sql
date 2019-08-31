package com.ursful.framework.orm.page;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SQLServerQueryPage extends AbstractQueryPage{

    @Override
    public DatabaseType databaseType() {
        return DatabaseType.SQLServer;
    }

    @Override
    public QueryInfo doQuery(IQuery query, Pageable page) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append(selectColumns(query, null));
        if(page != null){
            String byOrders = orders(query, null);
            if(!StringUtils.isEmpty(byOrders)){
                sb.append(" ,row_number() over(" + byOrders + ") rn_ ");
            }else{
                sb.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        sb.append(joins(query, values));
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        if(page != null){
            String tempSQL = sb.toString();
            sb = new StringBuffer("SELECT TOP " + page.getSize() +" * ");
            sb.append(" FROM (");
            sb.append(tempSQL);
            sb.append(") ms_ ");
            sb.append(" WHERE ms_.rn_ > " +page.getOffset()+" ");
        }else{
            sb.append(orders(query, null));
        }
        info.setClazz(query.getReturnClass());
        info.setSql(sb.toString());
        info.setValues(values);
        info.setColumns(query.getReturnColumns());

        return info;
    }


    @Override
    public SQLHelper doQuery(Class<?> clazz, String[] names, Terms terms, MultiOrder multiOrder, Integer start, Integer size) {
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT ");
        String nameStr = ORMUtils.join(names, ",");
        if(StringUtils.isEmpty(nameStr)){
            sql.append(Expression.EXPRESSION_ALL);
        }else{
            sql.append(nameStr);
        }
        if(start != null && size != null){
            String byOrders = null;
            if(multiOrder != null) {
                String orders = QueryUtils.getOrders(multiOrder.getOrders());
                if (!StringUtils.isEmpty(orders)) {
                    byOrders = " ORDER BY " + orders;
                }
            }
            if(byOrders != null){
                sql.append(" ,row_number() over(order by " + byOrders + ") rn_ ");
            }else{
                sql.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }
        sql.append(" FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(terms != null) {
            Condition condition = terms.getCondition();
            String conditions = QueryUtils.getConditions(clazz, ORMUtils.newList(condition), values);
            if (!StringUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }
        if(size != null && size.intValue() > 0){
            if(start == null){
                start = 0;
            }
            String tempSQL = sql.toString();
            sql = new StringBuffer("SELECT TOP " + size +" * ");
            sql.append(" FROM (");
            sql.append(tempSQL);
            sql.append(") ms_ ");
            sql.append(" WHERE ms_.rn_ > " + ((Math.max(1, start) - 1) * size)+" ");
        }else{
            String byOrders = null;
            if(multiOrder != null) {
                String orders = QueryUtils.getOrders(multiOrder.getOrders());
                if (!StringUtils.isEmpty(orders)) {
                    byOrders = " ORDER BY " + orders;
                }
            }
            if(byOrders != null){
                sql.append(byOrders);
            }
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;
    }


}
