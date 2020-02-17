package com.ursful.framework.orm.option;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public abstract class AbstractOptions implements Options{

    public abstract boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException;

    public void setParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException {
        boolean hasSet = preSetParameter(ps, connection, databaseType, i, obj, columnType, type);
        if(hasSet){
            return;
        }
        switch (type) {
            case BINARY:
                if(obj != null) {
                    if(obj instanceof byte[]) {
                        ps.setBinaryStream(i + 1, new ByteArrayInputStream((byte[]) obj));
                    }else{
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }else{
                    ps.setBinaryStream(i + 1, null);
                }
                break;
            case STRING:
                if(columnType == ColumnType.BINARY){
                    if(obj != null) {
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }else{
                        ps.setBinaryStream(i + 1, null);
                    }
                }else if(columnType == ColumnType.BLOB){
                    if(obj != null) {
                        try {
                            ps.setBlob(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }else{
                        ps.setBinaryStream(i + 1, null);
                    }
                }else if(columnType == ColumnType.CLOB){
                    Clob clob = null;
                    if(obj != null) {
                        clob = new SerialClob(obj.toString().toCharArray());
                    }
                    ps.setClob(i + 1, clob);
                }else {
                    if (obj != null && "".equals(obj.toString().trim())) {
                        obj = null;
                    }
                    ps.setString(i + 1, (String) obj);
                }
                break;
            case DATE:
                if(obj == null){
                    ps.setObject(i + 1, null);
                }else {
                    if(columnType == ColumnType.LONG) {
                        ps.setLong(i + 1, ((Date) obj).getTime());
                    }else if(columnType == ColumnType.DATETIME) {
                        Date date = (Date)obj;
                        ps.setTimestamp(i+1, new Timestamp(date.getTime()));
                    }else{//timestamp
                        ps.setTimestamp(i + 1, new Timestamp(((Date) obj).getTime()));
                    }
                }
                break;
            case DECIMAL:
                BigDecimal decimal = (BigDecimal) obj;
                BigDecimal setScale = decimal.setScale(5,BigDecimal.ROUND_HALF_DOWN);
                ps.setBigDecimal(i + 1, setScale);
                break;
            case DOUBLE:
                ps.setObject(i + 1, obj);
                break;
            default:
                ps.setObject(i + 1, obj);
                break;
        }
    }


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
                if(column.getAlias() == null && !StringUtils.isEmpty(alias)){
                    column.setAlias(alias);
                }
                inColumn.add(column.getAlias() + "." + column.getName());
                temp.add(QueryUtils.parseColumn(this,column));
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
                    String orderStr = QueryUtils.parseColumn(this,column);
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
        String orderString = QueryUtils.getOrders(this, orders);
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
        String whereCondition = QueryUtils.getConditions(this, query, conditions, values);
        if(whereCondition != null && !"".equals(whereCondition)){
            result =" WHERE " + whereCondition;
        }
        return result;
    }

    public String groups(IQuery query, String alias){
        String result = "";
        List<Column> columns = query.getGroups();
        QueryUtils.setColumnsAlias(columns, alias);
        String groupString = QueryUtils.getGroups(this, columns);
        if(groupString != null && !"".equals(groupString)){
            result =" GROUP BY " + groupString;
        }
        return result;
    }

    public String havings(IQuery query, List<Pair> values, String alias){
        String result = "";
        List<Condition> conditions = query.getHavings();
        QueryUtils.setConditionsAlias(conditions, alias);
        String havingString = QueryUtils.getConditions(this, query, conditions, values);
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

            String cdt = QueryUtils.getConditions(this, obj, temp, values);
            if(cdt != null && !"".equals(cdt)) {
                sb.append(" ON ");
                sb.append(cdt);
            }
        }
        return sb.toString();
    }

    protected String getForeignSQL(RdTable table, RdColumn rdColumn) {
        String foreignKey = rdColumn.foreignKey();
        String foreignTable= rdColumn.foreignTable();
        String foreignColumn = rdColumn.foreignColumn();
        Assert.isTrue(!StringUtils.isEmpty(foreignKey), "Foreign Key Name Should Not Be Empty.");
        Assert.isTrue(!StringUtils.isEmpty(foreignTable), "Foreign Table Should Not Be Empty.");
        Assert.isTrue(!StringUtils.isEmpty(foreignColumn), "Foreign Column Should Not Be Empty.");
//        constraint JOB_INST_EXEC_FK foreign key (JOB_INSTANCE_ID)
//                references BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
        return String.format("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)", foreignKey, rdColumn.name(), foreignTable, foreignColumn);
    }


    protected String getUniqueSQL(RdTable table, RdColumn rdColumn) {
        String uniqueName = null;
        if(StringUtils.isEmpty(uniqueName)){
            String [] tabs = table.name().split("_");
            StringBuffer names = new StringBuffer();
            for(String tab : tabs){
                if(tab.length() > 0) {
                    names.append(tab.charAt(0));
                }
            }
            if(rdColumn.uniqueKeys() != null && rdColumn.uniqueKeys().length > 0){
                for(String key : rdColumn.uniqueKeys()){
                    if(key.length() > 0) {
                        names.append("_" + key.charAt(0));
                    }
                }
            }else{
                names.append("_" + rdColumn.name().charAt(0));
            }
            uniqueName = names.toString().toUpperCase(Locale.ROOT);
        }else{
            uniqueName = rdColumn.uniqueName();
        }
        String uniqueKeys = ORMUtils.join(rdColumn.uniqueKeys(), ",");
        if(StringUtils.isEmpty(uniqueKeys)){
            uniqueKeys =  rdColumn.name();
        }
        return String.format("CONSTRAINT %s UNIQUE(%s)", uniqueName, uniqueKeys);
    }

    protected String columnComment(RdColumn rdColumn){
        String comment = null;
        if(!StringUtils.isEmpty(rdColumn.title())){
            comment =  rdColumn.title();
            if(!StringUtils.isEmpty(rdColumn.description())){
                comment += ";" + rdColumn.description();
            }
        }
        return comment;
    }


    protected String columnString(ColumnInfo info, RdColumn rdColumn, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        temp.append(info.getColumnName().toUpperCase(Locale.ROOT));
        temp.append(" ");

        String type = getColumnType(info, rdColumn);
        if(!StringUtils.isEmpty(rdColumn.defaultValue())){
            type += " DEFAULT '" +  rdColumn.defaultValue() + "'";
        }
        if(!rdColumn.nullable()){
            type += " NOT NULL";
        }
        temp.append(type);
        if(info.getPrimaryKey() && addKey){
            temp.append(" ");
            temp.append("PRIMARY KEY");
        }
        return temp.toString();
    }

    abstract String getColumnType(ColumnInfo info, RdColumn column);

}
