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
package com.ursful.framework.orm.option;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdForeignKey;
import com.ursful.framework.orm.annotation.RdUniqueKey;
import com.ursful.framework.orm.exception.TableAnnotationNotFoundException;
import com.ursful.framework.orm.exception.TableNameNotFoundException;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.orm.annotation.RdTable;
import org.springframework.util.StringUtils;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

public class MySQLOptions extends AbstractOptions{

    private AtomicInteger count = new AtomicInteger(0);

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Pair pair) throws SQLException {
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
            String conditions = getConditions(clazz, ORMUtils.newList(terms.getCondition()), values);
            if (!StringUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }
        if(multiOrder != null) {
            String orders = getOrders(multiOrder.getOrders());
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
            String sql = "SELECT * FROM information_schema.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            if(rs.next()) {
                return true;
            }
//            rs.close();
//            ps.close();
//            sql = "SELECT * FROM information_schema.TABLES WHERE  TABLE_NAME = ?  AND (TABLE_SCHEMA = ? OR TABLE_SCHEMA = ?)";
//            sql = ORMUtils.convertSQL(sql);
//            ps = connection.prepareStatement(sql);
//            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
//            ps.setString(2, dbName.toUpperCase(Locale.ROOT));
//            ps.setString(3, dbName.toLowerCase(Locale.ROOT));
//            rs = ps.executeQuery();
//            if(rs.next()) {
//                return true;
//            }
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

    public String getCaseSensitive(String name, int sensitive){
        if(name == null){
            return name;
        }
        if(RdTable.LOWER_CASE_SENSITIVE == sensitive){
            return name.toLowerCase(Locale.ROOT);
        }else if(RdTable.UPPER_CASE_SENSITIVE == sensitive){
            return name.toUpperCase(Locale.ROOT);
        }else if(RdTable.RESTRICT_CASE_SENSITIVE == sensitive){
            return name;
        }else{
            return name;//默认
        }
    }

    public String getTableName(RdTable rdTable) throws TableAnnotationNotFoundException, TableNameNotFoundException{
        if(rdTable == null){
            throw new TableAnnotationNotFoundException();
        }
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        if (StringUtils.isEmpty(tableName)){
            throw new TableNameNotFoundException();
        }
        return tableName;
    }

//            lower_case_table_names= 1  表名存储在磁盘是小写的，但是比较的时候是不区分大小写
//            lower_case_table_names=0  表名存储为给定的大小和比较是区分大小写的
//            lower_case_table_names=2, 表名存储为给定的大小写但是比较的时候是小写的
    @Override
    public Table table(Connection connection, RdTable rdTable) throws TableAnnotationNotFoundException, TableNameNotFoundException{
        if(rdTable == null){
            throw new TableAnnotationNotFoundException();
        }
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        if (StringUtils.isEmpty(tableName)){
            throw new TableNameNotFoundException();
        }
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            int type = -1;
            ps = connection.prepareStatement("show variables like 'lower_case_table_names'");
            rs = ps.executeQuery();
            if(rs.next()){
                type = rs.getInt(2);
            }
            rs.close();
            ps.close();
            String dbName = connection.getCatalog();
            String sql = "SELECT TABLE_NAME,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_NAME LIKE ? AND TABLE_SCHEMA = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            if(rs.next()) {
                String newTableName = rs.getString(1);
                if(type == 1) {
                    newTableName = tableName;
                }
                return new Table(newTableName, rs.getString(2));
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
    public List<TableColumn> columns(Connection connection, RdTable rdTable) throws TableAnnotationNotFoundException, TableNameNotFoundException{
        if(rdTable == null){
            throw new TableAnnotationNotFoundException();
        }
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        if (StringUtils.isEmpty(tableName)){
            throw new TableNameNotFoundException();
        }
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME LIKE ? AND   TABLE_SCHEMA = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            while (rs.next()){
                String cname = rs.getString("COLUMN_NAME");
                TableColumn column = new TableColumn(tableName, cname);
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
        String tableName = getCaseSensitive(table.name(), table.sensitive());
        List<String> sqls = new ArrayList<String>();
        if(table.dropped()){
            if(tableExisted){
                if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                    sqls.add(String.format("DROP TABLE %s", tableName));
                }else{
                    sqls.add(String.format("DROP TABLE `%s`", tableName));
                }
            }
        }else{
            //create table
            if(tableExisted){
                Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
                 /*
                    lower_case_table_names： 此参数不可以动态修改，必须重启数据库
                    lower_case_table_names = 1  表名存储在磁盘是小写的，但是比较的时候是不区分大小写
                    lower_case_table_names=0  表名存储为给定的大小和比较是区分大小写的
                    lower_case_table_names=2, 表名存储为给定的大小写但是比较的时候是小写的
                 */
                int sensitive = -1;
                for (TableColumn column : tableColumns){
                    columnMap.put(column.getColumn(), column);
                    if(sensitive == -1){
//                        sensitive = column.getSensitive();
                    }
                }
                // add or drop, next version modify.
                for(ColumnInfo info : infos){
                    String columnName =  getCaseSensitive(info.getColumnName(), table.sensitive());
                    TableColumn tableColumn = null;
                    if(sensitive == 1){
                        tableColumn = columnMap.get(columnName.toLowerCase(Locale.ROOT));
                    }else{
                        tableColumn = columnMap.get(columnName);
                    }
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
                            }else{
                                sqls.add(String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, columnName));
                            }
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
                                String temp = columnString(info, table.sensitive(), rdColumn, false);
                                String comment = columnComment(rdColumn);
                                if (!StringUtils.isEmpty(comment)) {
                                    temp += " COMMENT '" + comment + "'";
                                }
                                if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("ALTER TABLE %s MODIFY COLUMN %s", tableName, temp));
                                }else{
                                    sqls.add(String.format("ALTER TABLE `%s` MODIFY COLUMN %s", tableName, temp));
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                            RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, table.sensitive(), rdColumn, true);
                            String comment = columnComment(rdColumn);
                            if (!StringUtils.isEmpty(comment)) {
                                temp += " COMMENT'" + comment + "'";
                            }
                            if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s ADD COLUMN %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE `%s` ADD COLUMN %s", tableName, temp));
                            }
                            if(!info.getPrimaryKey()) {
                                String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                                if(uniqueSQL != null) {
                                    sqls.add("ALTER TABLE `" + tableName + "` ADD " + uniqueSQL);
                                }
                            }
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                sqls.add("ALTER TABLE `" + tableName + "` ADD " + foreignSQL);
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
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    String temp = columnString(info, table.sensitive(), rdColumn, true);
                    String comment = columnComment(rdColumn);
                    if(!StringUtils.isEmpty(comment)){
                        temp += " COMMENT '" + comment + "'";
                    }
                    columnSQL.add(temp.toString());
                    if(!info.getPrimaryKey()) {
                        String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                        if(uniqueSQL != null) {
                            columnSQL.add(uniqueSQL);
                        }
                    }
                    String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                    if(foreignSQL != null){
                        columnSQL.add(foreignSQL);
                    }
                }
                sql.append(ORMUtils.join(columnSQL, ","));
                sql.append(")");
                String comment = table.comment();
                if(StringUtils.isEmpty(comment)){
                    comment = table.title();
                }
                if(!StringUtils.isEmpty(comment)){
                    sql.append(" COMMENT='" + comment + "' ");
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

    protected String columnString(ColumnInfo info, int sensitive, RdColumn rdColumn, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(info.getColumnName(), sensitive);
        if(sensitive == RdTable.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("`%s`", cname));
        }
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
