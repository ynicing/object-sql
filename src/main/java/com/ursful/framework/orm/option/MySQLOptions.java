package com.ursful.framework.orm.option;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class MySQLOptions extends AbstractOptions{

    private AtomicInteger count = new AtomicInteger(0);

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException {
        return false;
    }

    @Override
    public String keyword() {
        return "mysql";
    }

    @Override
    public String getColumnWithOperator(OperatorType operatorType, String name, String value) {
        String result = null;
        switch (operatorType){
            case NOT:
                result = "(" + operatorType.getOperator() + name + ")";
                break;
        }
        return result;
    }

    @Override
    public String getColumnWithOperatorAndFunction(String function, boolean inFunction, OperatorType operatorType, String name, String value) {
        String result = null;
        switch (operatorType){
            case NOT:
                if(inFunction) {// SUM(~NAME)
                    result = function + "(" + operatorType.getOperator() + name + ")";
                }else{// ~SUM(name)
                    result = "(" + operatorType.getOperator() + function + "(" + name + "))";
                }
                break;
        }
        if(result == null){
            if(inFunction) {
                result = function + "(" + name + operatorType.getOperator() + value + ")";
            }else{
                result = "(" + function + "(" + name + ")"  + operatorType.getOperator() + value + ")";
            }
        }
        return result;
    }

    @Override
    public String databaseType() {
        return "MySQL";
    }

    @Override
    public String nanoTimeSQL() {
        return "SELECT NOW(3)";
    }

    @Override
    public QueryInfo doQuery(IQuery query, Pageable page) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append(selectColumns(query, null));
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        sb.append(joins(query, values));
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        sb.append(orders(query, null));
        if(page != null){
            sb.append(" LIMIT ? OFFSET ? ");
            values.add(new Pair(new Integer(page.getSize())));
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

        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT ");
        String nameStr = ORMUtils.join(names, ",");
        if(StringUtils.isEmpty(nameStr)){
           sql.append(Expression.EXPRESSION_ALL);
        }else{
            sql.append(nameStr);
        }
        sql.append(" FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(terms != null) {
            String conditions = QueryUtils.getConditions(this, clazz, ORMUtils.newList(terms.getCondition()), values);
            if (!StringUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }
        if(multiOrder != null) {
            String orders = QueryUtils.getOrders(this, multiOrder.getOrders());
            if (!StringUtils.isEmpty(orders)) {
                sql.append(" ORDER BY " + orders);
            }
        }
        if(size != null && size.intValue() > 0){
            if(start == null){
                start = 0;
            }
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
