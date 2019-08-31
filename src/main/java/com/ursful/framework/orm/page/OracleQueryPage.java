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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

public class OracleQueryPage extends AbstractQueryPage{

    @Override
    public DatabaseType databaseType() {
        return DatabaseType.ORACLE;
    }

    @Override
    public QueryInfo doQuery(IQuery query, Pageable page) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();

        String alias = "ora_";

        sb.append("SELECT ");
        sb.append(selectColumns(query, alias));
        String order = orders(query, alias);
        boolean hasOrder = !StringUtils.isEmpty(order);
        if(page != null && !hasOrder){
            sb.append(",ROWNUM rn_ ");
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, alias));
        sb.append(joins(query, values));
        int now = sb.length();
        sb.append(wheres(query, values, null));
        int then = sb.length();
        boolean hasCondition = then > now;
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        sb.append(order);
        if(page != null){
            if(hasOrder){
                sb = new StringBuffer("SELECT ora_a_.* FROM (SELECT ora_b_.*,ROWNUM rn_  FROM (" + sb.toString() + ") ora_b_ WHERE ROWNUM <= ?) ora_a_ WHERE ora_a_.rn_ > ? ");
            }else{
                sb = new StringBuffer("SELECT ora_a_.* FROM (" + sb.toString() + (hasCondition?" AND " : " WHERE ") + "ROWNUM <= ? ) ora_a_ WHERE ora_a_.rn_ > ? ");
            }
            values.add(new Pair(new Integer(page.getSize() + page.getOffset())));
            values.add(new Pair(new Integer(page.getOffset())));
        }
        info.setClazz(query.getReturnClass());
        info.setSql(sb.toString());
        info.setValues(values);
        info.setColumns(query.getReturnColumns());

        return info;
    }

    @Override
    public SQLHelper doQuery(Class<?> clazz, String[] names, Terms terms, MultiOrder multiOrder, Integer start, Integer size) {
        String alias = "ora_";
        if(names != null){
            String [] tempNames = new String[names.length];
            for(int i = 0; i < tempNames.length; i++){
                tempNames[i] = alias + "." + names[i];
            }
            names = tempNames;
        }
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT ");
        String nameStr = ORMUtils.join(names, ",");
        if(StringUtils.isEmpty(nameStr)){
            sql.append(alias + "." + Expression.EXPRESSION_ALL);
        }else{
            sql.append(nameStr);
        }
        boolean hasOrder = false;
        String order = "";
        if(multiOrder != null) {
            QueryUtils.setMultiOrderAlias(multiOrder, alias);
            String orders = QueryUtils.getOrders(multiOrder.getOrders());
            if (!StringUtils.isEmpty(orders)) {
                hasOrder = true;
                order = " ORDER BY " + orders;
            }
        }
        if(start != null && size != null && !hasOrder){
            sql.append(",ROWNUM rn_ ");
        }
        sql.append(" FROM " + tableName + " " + alias);
        List<Pair> values = new ArrayList<Pair>();
        boolean hasCondition = false;

        if(terms != null) {
            Condition condition = terms.getCondition();
            QueryUtils.setConditionAlias(condition, alias);
            String conditions = QueryUtils.getConditions(clazz, ORMUtils.newList(condition), values);
            if (!StringUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
                hasCondition = true;
            }
        }
        sql.append(order);
        if(size != null && size.intValue() > 0){
            if(start == null){
                start = 0;
            }
            if(hasOrder){
                sql = new StringBuffer("SELECT ora_a_.* FROM (SELECT ora_b_.*,ROWNUM rn_ FROM (" + sql.toString() + ") ora_b_ WHERE ROWNUM <= ?) ora_a_ WHERE ora_a_.rn_ > ? ");
            }else{
                sql = new StringBuffer("SELECT ora_a_.* FROM (" + sql.toString() + (hasCondition?" AND " : " WHERE ") + " ROWNUM <= ? ) ora_a_ WHERE ora_a_.rn_ > ? ");
            }
            values.add(new Pair( ((Math.max(1, start)) * size)));
            values.add(new Pair( ((Math.max(1, start) - 1) * size)));
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;
    }
}
