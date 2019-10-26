package com.ursful.framework.orm.option;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;
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

    @Override
    public boolean tableExists(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.TABLES WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) AND (TABLE_SCHEMA = ? OR TABLE_SCHEMA = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            ps.setString(3, dbName.toUpperCase(Locale.ROOT));
            ps.setString(4, dbName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            if(rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }
        return false;
    }

    @Override
    public Table table(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.TABLES WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) AND (TABLE_SCHEMA = ? OR TABLE_SCHEMA = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            ps.setString(3, dbName.toUpperCase(Locale.ROOT));
            ps.setString(4, dbName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            if(rs.next()) {
                return new Table(rs.getString("TABLE_NAME"), rs.getString("TABLE_COMMENT"));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }
        return null;
    }

    @Override
    public List<TableColumn> columns(Connection connection, String tableName) {
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.COLUMNS WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) AND (TABLE_SCHEMA = ? OR TABLE_SCHEMA = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            ps.setString(3, dbName.toUpperCase(Locale.ROOT));
            ps.setString(4, dbName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            while (rs.next()){
                TableColumn column = new TableColumn(rs.getString("TABLE_NAME").toUpperCase(Locale.ROOT), rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
                column.setType(rs.getString("DATA_TYPE").toUpperCase(Locale.ROOT));
                column.setLength(rs.getInt("CHARACTER_MAXIMUM_LENGTH"));
                column.setNullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                column.setPrecision(rs.getInt("NUMERIC_PRECISION"));
                column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
                column.setScale(rs.getInt("NUMERIC_SCALE"));
                column.setOrder(rs.getInt("ORDINAL_POSITION"));
                column.setComment(rs.getString("COLUMN_COMMENT"));
                columns.add(column);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(ps != null){
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if(rs != null){
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }
        return columns;
    }


    @Override
    public List<String> manageTable(RdTable table, List<ColumnInfo> infos, boolean tableExisted, List<TableColumn> tableColumns) {
        List<String> sqls = new ArrayList<String>();
        if(table.dropped()){
            if(tableExisted){
                sqls.add(String.format("DROP TABLE %s", table.name().toUpperCase(Locale.ROOT)));
            }
        }else{
            String tableName = table.name().toUpperCase(Locale.ROOT);
            //create table
            if(tableExisted){
                Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
                for (TableColumn column : tableColumns){
                    columnMap.put(column.getColumn().toUpperCase(Locale.ROOT), column);
                }
                // add or drop, next version modify.
                for(ColumnInfo info : infos){
                    TableColumn tableColumn = columnMap.get(info.getColumnName().toUpperCase(Locale.ROOT));
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", table.name().toUpperCase(Locale.ROOT), rdColumn.name().toUpperCase(Locale.ROOT)));
                        }else{
                            boolean needUpdate = false;
                            if("VARCHAR".equalsIgnoreCase(tableColumn.getType()) || "CHAR".equalsIgnoreCase(tableColumn.getType())){
                                if(tableColumn.getLength() == null){
                                    continue;
                                }
                                if(tableColumn.getLength().intValue() != rdColumn.length()){
                                    needUpdate = true;
                                }
                            }else if("DECIMAL".equalsIgnoreCase(tableColumn.getType())){
                                if(tableColumn.getPrecision() == null || tableColumn.getScale() == null){
                                    continue;
                                }
                                if("Date".equalsIgnoreCase(info.getType())){
                                    if ((tableColumn.getPrecision().intValue() != rdColumn.datePrecision()) || (tableColumn.getScale().intValue() != 0)) {
                                        needUpdate = true;
                                    }
                                }else {
                                    if ((tableColumn.getPrecision().intValue() != rdColumn.precision()) || (tableColumn.getScale().intValue() != rdColumn.scale())) {
                                        needUpdate = true;
                                    }
                                }
                            }else{
                                String type = getColumnType(info, rdColumn).toUpperCase(Locale.ROOT);
                                if(!type.startsWith(tableColumn.getType().toUpperCase(Locale.ROOT))){
                                    needUpdate = true;
                                }
                            }
                            if(!needUpdate && !StringUtils.isEmpty(tableColumn.getDefaultValue())){
                                if(!tableColumn.getDefaultValue().equals(rdColumn.defaultValue())){
                                    needUpdate = true;
                                }
                            }
                            if(needUpdate) {
                                String temp = columnString(info, rdColumn, false);
                                String comment = columnComment(rdColumn);
                                if (!StringUtils.isEmpty(comment)) {
                                    temp += " COMMENT'" + comment + "'";
                                }
                                sqls.add(String.format("ALTER TABLE %s MODIFY COLUMN %s", table.name().toUpperCase(Locale.ROOT), temp));
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, rdColumn, true);
                            String comment = columnComment(rdColumn);
                            if (!StringUtils.isEmpty(comment)) {
                                temp += " COMMENT'" + comment + "'";
                            }
                            sqls.add(String.format("ALTER TABLE %s ADD COLUMN %s", tableName, temp));
                            if (rdColumn.unique() && !info.getPrimaryKey()) {
                                String uniqueSQL = getUniqueSQL(table, rdColumn);
                                sqls.add("ALTER TABLE " + tableName + " ADD " + uniqueSQL);
                            }
                        }
                    }
                }
            }else{
                StringBuffer sql = new StringBuffer();
                sql.append("CREATE TABLE " + tableName + "(");
                List<String> columnSQL = new ArrayList<String>();
                for(int i = 0; i < infos.size(); i++){
                    ColumnInfo info = infos.get(i);
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    String temp = columnString(info, rdColumn, true);
                    String comment = columnComment(rdColumn);
                    if(!StringUtils.isEmpty(comment)){
                        temp += " COMMENT '" + comment + "'";
                    }
                    columnSQL.add(temp.toString());
                    if(rdColumn.unique() && !info.getPrimaryKey()) {
                        //	CONSTRAINT `SYS_RESOURCE_URL_METHOD` UNIQUE(`URL`, `REQUEST_METHOD`)
                        String uniqueSQL = getUniqueSQL(table, rdColumn);
                        columnSQL.add(uniqueSQL);
                    }
                }
                sql.append(ORMUtils.join(columnSQL, ","));
                sql.append(")");
                if(!StringUtils.isEmpty(table.title())){
                    sql.append(" COMMENT='" + table.title() + "' ");
                }
                if(!StringUtils.isEmpty(table.collate())){
                    sql.append(" COLLATE='" + table.collate() + "'");
                }
                if(!StringUtils.isEmpty(table.engine())){
                    sql.append(" ENGINE='" + table.engine() + "'");
                }
                sql.append(";");
                sqls.add(sql.toString());
            }
        }
        return sqls;
    }

    @Override
    protected String getColumnType(ColumnInfo info, RdColumn rdColumn) {
        String type = "";
        String infoType = info.getField().getType().getName();
        if(String.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.TEXT){
                type = "TEXT";
            }else if(info.getColumnType() == ColumnType.BLOB){
                type = "BLOB";
            }else if(info.getColumnType() == ColumnType.CLOB) {
                type = "TEXT";
            }else if(info.getColumnType() == ColumnType.BINARY){
                type = "VARBINARY(" + rdColumn.length() + ")";
            }else if(info.getColumnType() == ColumnType.CHAR){
                type = "CHAR(" + rdColumn.length() + ")";
            }else{
                type = "VARCHAR(" + rdColumn.length()  + ")";
            }
        }else if (Integer.class.getName().equals(infoType)) {
            type = "INT(" + rdColumn.precision() + ")";
        }else if(Date.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.LONG){
                type = "DECIMAL(" + rdColumn.datePrecision() + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATETIME";
            }else{
                throw new RuntimeException("Not support type : " + infoType + "," + info.getColumnType().name());
            }
        }else if(Long.class.getName().equals(infoType)){
            type = "BIGINT(" + rdColumn.precision() + ")";
        }else if(Double.class.getName().equals(infoType)){
            type = "DOUBLE";
        }else if(Float.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.REAL){
                type = "REAL";
            }else {
                type = "FLOAT";
            }
        }else if(BigDecimal.class.getName().equals(infoType)){
            type = "DECIMAL(" + rdColumn.precision() + "," + rdColumn.scale() + ")";
        }else if(byte[].class.getName().equals(infoType)){
            type = "VARBINARY(" + rdColumn.length() + ")";
        }else{
            throw new RuntimeException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }
}
