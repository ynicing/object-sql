/*
 * Copyright 2017 @objectsql.com
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
package com.objectsql.option;

import com.objectsql.IMultiQuery;
import com.objectsql.IQuery;
import com.objectsql.annotation.*;
import com.objectsql.exception.ORMException;
import com.objectsql.helper.SQLHelper;
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;

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
        Map<String, String> asNames = new HashMap<String, String>();
        sb.append(selectColumns(query, null, asNames, values));
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        if(query instanceof IMultiQuery) {
            sb.append(joins((IMultiQuery)query, values));
        }
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        sb.append(orders(query, null, asNames));
        if(page != null){
            sb.append(" LIMIT ? OFFSET ? ");
            values.add(new Pair(new Integer(page.getSize())));
            values.add(new Pair(new Integer(page.getOffset())));
        }
        info.setClazz(query.getReturnClass());
        info.setSql(sb.toString());
        info.setValues(values);
        info.setColumns(query.getFinalReturnColumns());
        if (TextTransformType.LOWER == query.textTransformType()){
            info.setSql(info.getSql().toLowerCase(Locale.ROOT));
        }else if(TextTransformType.UPPER == query.textTransformType()){
            info.setSql(info.getSql().toUpperCase(Locale.ROOT));
        }
        return info;
    }

    @Override
    public SQLHelper doQuery(Class<?> clazz, String[] names, Condition condition, MultiOrder multiOrder, Integer start, Integer size) {

        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT ");
        String nameStr = ORMUtils.join(names, ",");
        if(ORMUtils.isEmpty(nameStr)){
            sql.append(Column.ALL);
        }else{
            sql.append(nameStr);
        }
        sql.append(" FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(condition != null) {
            String conditions = getConditions(clazz, ORMUtils.newList(condition), values);
            if (!ORMUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }
        if(multiOrder != null) {
            String orders = getOrders(multiOrder.getOrders(), clazz);
            if (!ORMUtils.isEmpty(orders)) {
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

    private List<String> tableConstraints(Connection connection){
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<String>();
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT CONSTRAINT_NAME AS INDEX_NAME FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE TABLE_SCHEMA = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, dbName);
            rs = ps.executeQuery();
            while(rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if(indexName != null) {
                    names.add(indexName.toUpperCase(Locale.ROOT));
                }
            }
        } catch (SQLException e) {
            throw new ORMException(e);
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
        return names;
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
            throw new ORMException(e);
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
        if(Table.LOWER_CASE_SENSITIVE == sensitive){
            return name.toLowerCase(Locale.ROOT);
        }else if(Table.UPPER_CASE_SENSITIVE == sensitive){
            return name.toUpperCase(Locale.ROOT);
        }else if(Table.RESTRICT_CASE_SENSITIVE == sensitive){
            return name;
        }else{
            return name;//默认
        }
    }

    @Override
    public List<Table> tables(Connection connection, String keyword) {
        List<Table> temp = new ArrayList<Table>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT TABLE_NAME,TABLE_COMMENT FROM information_schema.TABLES WHERE TABLE_SCHEMA = ? ";
            if(!ORMUtils.isEmpty(keyword)){
                sql +=  "AND TABLE_NAME LIKE ? ";
            }
            ps = connection.prepareStatement(sql);
            ps.setString(1, dbName);
            if(!ORMUtils.isEmpty(keyword)){
                ps.setString(2, "%" + keyword + "%");
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                temp.add(new Table(rs.getString(1), rs.getString(2)));
            }
        } catch (SQLException e) {
            throw new ORMException(e);
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
        return temp;

    }

    //            lower_case_table_names= 1  表名存储在磁盘是小写的，但是比较的时候是不区分大小写
//            lower_case_table_names=0  表名存储为给定的大小和比较是区分大小写的
//            lower_case_table_names=2, 表名存储为给定的大小写但是比较的时候是小写的
    @Override
    public Table table(Connection connection, RdTable rdTable) throws ORMException{
        String tableName = getTableName(rdTable);
        Table table = new Table(tableName);
        table.setSensitive(rdTable.sensitive());
        return  table(connection, table);
    }

    @Override
    public Table table(Connection connection, Table table){
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
            String tableName = getCaseSensitive(table.getName(), table.getSensitive());
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
            throw new ORMException(e);
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
    public List<TableColumn> columns(Connection connection, RdTable rdTable) throws ORMException{
        String tableName = getTableName(rdTable);
        return columns(connection, tableName);
    }

    @Override
    public List<TableColumn> columns(Connection connection, String tableName){
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME LIKE ? AND   TABLE_SCHEMA = ? ORDER BY ORDINAL_POSITION ASC";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            while (rs.next()){
                String cname = rs.getString("COLUMN_NAME");
                TableColumn column = new TableColumn(tableName, cname);
                column.setType(rs.getString("DATA_TYPE").toUpperCase(Locale.ROOT));
                column.setLength(rs.getLong("CHARACTER_MAXIMUM_LENGTH"));
                column.setNullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                column.setPrecision(rs.getInt("NUMERIC_PRECISION"));
                column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
                column.setScale(rs.getInt("NUMERIC_SCALE"));
                column.setOrder(rs.getInt("ORDINAL_POSITION"));
                column.setComment(rs.getString("COLUMN_COMMENT"));
                column.setIsPrimaryKey("PRI".equalsIgnoreCase(rs.getString("COLUMN_KEY")));
                columns.add(column);
            }
        } catch (SQLException e) {
            throw new ORMException(e);
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
    public List<String> createOrUpdateSqls(Connection connection, RdTable table, List<ColumnInfo> infos, boolean tableExisted, List<TableColumn> tableColumns) {
        String tableName = getCaseSensitive(table.name(), table.sensitive());

        List<String> constraints = tableConstraints(connection);

        List<String> sqls = new ArrayList<String>();
        if(table.dropped()){
            if(tableExisted){
                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
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
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
                            }else{
                                sqls.add(String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, columnName));
                            }
                        }else{
                            boolean needUpdate = false;
                            boolean isNumber = false;
                            if("VARCHAR".equalsIgnoreCase(tableColumn.getType()) || "CHAR".equalsIgnoreCase(tableColumn.getType())){
                                if(tableColumn.getLength() == null){
                                    continue;
                                }
                                if(tableColumn.getLength().intValue() != rdColumn.length()){
                                    needUpdate = true;
                                }
                            }else if("DECIMAL".equalsIgnoreCase(tableColumn.getType())){
                                isNumber = true;
                                if(tableColumn.getPrecision() == null || tableColumn.getScale() == null){
                                    continue;
                                }
                                if("Date".equalsIgnoreCase(info.getType())){
                                    if ((tableColumn.getPrecision().intValue() != rdColumn.precision()) || (tableColumn.getScale().intValue() != 0)) {
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
                                    //REAL就是DOUBLE ，如果SQL服务器模式包括REAL_AS_FLOAT选项，REAL是FLOAT的同义词而不是DOUBLE的同义词
                                    if("real".equalsIgnoreCase(type) && ("double".equalsIgnoreCase(tableColumn.getType())|| "float".equalsIgnoreCase(tableColumn.getType()))){
                                        needUpdate = false;
                                        isNumber = true;
                                    }else{
                                        needUpdate = true;
                                    }
                                }
                            }
                            if(!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())){
                                if (isNumber){
                                    if (ORMUtils.isEmpty(rdColumn.defaultValue())){
                                        needUpdate = true;
                                    }else{
                                        needUpdate = new BigDecimal(tableColumn.getDefaultValue()).compareTo(
                                                new BigDecimal(rdColumn.defaultValue())) != 0;
                                    }
                                }else {
                                    if (!tableColumn.getDefaultValue().equals(rdColumn.defaultValue())) {
                                        //MySQL timestamp 默认 NOT NULL, 同时默认值为 CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                                        needUpdate = true;
                                    }
                                }
                            }
                            RdId rdId = info.getField().getAnnotation(RdId.class);
                            if (!needUpdate && ((tableColumn.isNullable() && !rdColumn.nullable())||(!tableColumn.isNullable() && rdColumn.nullable())) && !tableColumn.isPrimaryKey() && rdId == null){
                                needUpdate = true;
                            }
                            if(needUpdate) {
                                String temp = columnString(info, table.sensitive(), rdColumn, false);
                                String comment = columnComment(rdColumn);
                                if (!ORMUtils.isEmpty(comment)) {
                                    temp += " COMMENT '" + comment + "'";
                                }
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("ALTER TABLE %s MODIFY COLUMN %s", tableName, temp));
                                }else{
                                    sqls.add(String.format("ALTER TABLE `%s` MODIFY COLUMN %s", tableName, temp));
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, table.sensitive(), rdColumn, true);
                            String comment = columnComment(rdColumn);
                            if (!ORMUtils.isEmpty(comment)) {
                                temp += " COMMENT'" + comment + "'";
                            }
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s ADD COLUMN %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE `%s` ADD COLUMN %s", tableName, temp));
                            }

                        }
                    }
                    if(!info.getPrimaryKey() && uniqueKey != null) {
                        if(!constraints.contains(uniqueKey.name().toUpperCase(Locale.ROOT))){
                            String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                            if(uniqueSQL != null) {
                                sqls.add("ALTER TABLE `" + tableName + "` ADD " + uniqueSQL);
                            }
                        }
                    }
                    if (foreignKey != null) {
                        if (!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))) {
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if (foreignSQL != null) {
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
                    if(!ORMUtils.isEmpty(comment)){
                        temp += " COMMENT '" + comment + "'";
                    }
                    columnSQL.add(temp.toString());
                    if(!info.getPrimaryKey() && uniqueKey != null) {
                        if(!constraints.contains(uniqueKey.name().toUpperCase(Locale.ROOT))){
                            String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                            if(uniqueSQL != null) {
                                columnSQL.add(uniqueSQL);
                            }
                        }
                    }
                    if (foreignKey != null) {
                        if (!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))) {
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if (foreignSQL != null) {
                                columnSQL.add(foreignSQL);
                            }
                        }
                    }
                }
                sql.append(ORMUtils.join(columnSQL, ","));
                sql.append(")");
                String comment = table.comment();
                if(!ORMUtils.isEmpty(comment)){
                    sql.append(" COMMENT='" + comment + "' ");
                }
                if(!ORMUtils.isEmpty(table.collate())){
                    sql.append(" COLLATE='" + table.collate() + "'");
                }
                if(!ORMUtils.isEmpty(table.engine())){
                    sql.append(" ENGINE='" + table.engine() + "'");
                }
                sql.append(";");
                sqls.add(sql.toString());
            }
        }
        return sqls;
    }

    @Override
    public String dropTable(Table table){
        if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
            return String.format("DROP TABLE %s", table.getName());
        }else {
            return String.format("DROP TABLE `%s`", table.getName());
        }
    }

    @Override
    public List<String> createOrUpdateSqls(Connection connection, Table table, List<TableColumn> columns, List<TableColumn> tableColumns, boolean tableExisted) {
        String tableName = getCaseSensitive(table.getName(), table.getSensitive());

        List<String> constraints = tableConstraints(connection);

        List<String> sqls = new ArrayList<String>();

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
            }
            // add or drop, next version modify.
            for(TableColumn tc : columns){
                String columnName =  getCaseSensitive(tc.getColumn(), table.getSensitive());
                TableColumn tableColumn = null;
                if(sensitive == 1){
                    tableColumn = columnMap.get(columnName.toLowerCase(Locale.ROOT));
                }else{
                    tableColumn = columnMap.get(columnName);
                }
                if(tableColumn != null){
                    if(tc.isDropped()){
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                            sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
                        }else{
                            sqls.add(String.format("ALTER TABLE `%s` DROP COLUMN `%s`", tableName, columnName));
                        }
                    }else {
                        boolean needUpdate = false;
                        if ("VARCHAR".equalsIgnoreCase(tableColumn.getType()) || "CHAR".equalsIgnoreCase(tableColumn.getType())) {
                            if (tableColumn.getLength() == null) {
                                continue;
                            }
                            if (tableColumn.getLength().intValue() != tc.getLength()) {
                                needUpdate = true;
                            }
                        } else if ("DECIMAL".equalsIgnoreCase(tableColumn.getType())) {
                            if (tableColumn.getPrecision() == null || tableColumn.getScale() == null) {
                                continue;
                            }
                            if ("Date".equalsIgnoreCase(tc.getType())) {
                                if ((tableColumn.getPrecision().intValue() != tc.getPrecision()) || (tableColumn.getScale().intValue() != 0)) {
                                    needUpdate = true;
                                }
                            } else {
                                if ((tableColumn.getPrecision().intValue() != tc.getPrecision()) || (tableColumn.getScale().intValue() != tc.getScale())) {
                                    needUpdate = true;
                                }
                            }
                        } else {
                            String type = tc.getType().toUpperCase(Locale.ROOT);
                            if (!type.startsWith(tableColumn.getType().toUpperCase(Locale.ROOT))) {
                                needUpdate = true;
                            }
                        }
                        if (!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())) {
                            if (!tableColumn.getDefaultValue().equals(tc.getDefaultValue())) {
                                needUpdate = true;
                            }
                        }
                        if (!needUpdate && (tableColumn.isNullable() != tc.isNullable()) && !tableColumn.isPrimaryKey() && !tc.isPrimaryKey()) {
                            needUpdate = true;
                        }
                        if (needUpdate) {
                            String temp = columnString(tableColumn, table.getSensitive(), false);
                            String comment = tableColumn.getComment();
                            if (!ORMUtils.isEmpty(comment)) {
                                temp += " COMMENT '" + comment + "'";
                            }
                            if (table.getSensitive() == Table.DEFAULT_SENSITIVE) {
                                sqls.add(String.format("ALTER TABLE %s MODIFY COLUMN %s", tableName, temp));
                            } else {
                                sqls.add(String.format("ALTER TABLE `%s` MODIFY COLUMN %s", tableName, temp));
                            }
                        }
                    }
                }else{
                    if(!tc.isDropped()){
                        // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                        String temp = columnString(tc, table.getSensitive(), true);
                        String comment = tc.getComment();
                        if (!ORMUtils.isEmpty(comment)) {
                            temp += " COMMENT'" + comment + "'";
                        }
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                            sqls.add(String.format("ALTER TABLE %s ADD COLUMN %s", tableName, temp));
                        }else{
                            sqls.add(String.format("ALTER TABLE `%s` ADD COLUMN %s", tableName, temp));
                        }
                    }
                }
            }
        }else{
            StringBuffer sql = new StringBuffer();
            sql.append("CREATE TABLE " + tableName + "(");
            List<String> columnSQL = new ArrayList<String>();
            for(int i = 0; i < columns.size(); i++){
                TableColumn tc = columns.get(i);
                String temp = columnString(tc, table.getSensitive(), true);
                String comment = tc.getComment();
                if(!ORMUtils.isEmpty(comment)){
                    temp += " COMMENT '" + comment + "'";
                }
                columnSQL.add(temp.toString());
            }
            sql.append(ORMUtils.join(columnSQL, ","));
            sql.append(")");
            String comment = table.getComment();
            if(!ORMUtils.isEmpty(comment)){
                sql.append(" COMMENT='" + comment + "' ");
            }
            if(!ORMUtils.isEmpty(table.getCollate())){
                sql.append(" COLLATE='" + table.getCollate() + "'");
            }
            if(!ORMUtils.isEmpty(table.getEngine())){
                sql.append(" ENGINE='" + table.getEngine() + "'");
            }
            sql.append(";");
            sqls.add(sql.toString());
        }
        return sqls;
    }

    protected String columnString(TableColumn tableColumn, int sensitive, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(tableColumn.getColumn(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("`%s`", cname));
        }
        String it = getColumnType(tableColumn);
        String type = " " + it;
        if(!ORMUtils.isEmpty(tableColumn.getDefaultValue())){
            type += " DEFAULT '" +  tableColumn.getDefaultValue() + "'";
        }
        if(!tableColumn.isNullable()){
            type += " NOT NULL";
        }else{
            //MySQL timestamp 默认 NOT NULL, 同时默认值为 CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            if("timestamp".equalsIgnoreCase(it)){
                type += " NULL";
            }
        }
        temp.append(type);
        if(tableColumn.isPrimaryKey() && addKey){
            temp.append(" PRIMARY KEY");
        }
        if(tableColumn.isPrimaryKey() && tableColumn.isAutoIncrement()){
            temp.append(" AUTO_INCREMENT");
        }
        return temp.toString();
    }

    protected String columnString(ColumnInfo info, int sensitive, RdColumn rdColumn, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(info.getColumnName(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("`%s`", cname));
        }
        String it = getColumnType(info, rdColumn);
        String type = " " + it;
        if(!ORMUtils.isEmpty(rdColumn.defaultValue())){
            type += " DEFAULT '" +  rdColumn.defaultValue() + "'";
        }
        if(!rdColumn.nullable()){
            type += " NOT NULL";
        }else{
            //MySQL timestamp 默认 NOT NULL, 同时默认值为 CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            if("timestamp".equalsIgnoreCase(it)){
                type += " NULL";
            }
        }
        temp.append(type);
        if(info.getPrimaryKey() && addKey){
            temp.append(" PRIMARY KEY");
        }
        RdId rdId = info.getField().getAnnotation(RdId.class);
        if(rdId != null && rdId.autoIncrement()){
            temp.append(" AUTO_INCREMENT");
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
            }else if(info.getColumnType() == ColumnType.MEDIUM_TEXT){
                type = "MEDIUMTEXT";
            }else if(info.getColumnType() == ColumnType.LONG_TEXT){
                type = "LONGTEXT";
            }else if(info.getColumnType() == ColumnType.BLOB){
                type = "BLOB";
            //hos no clob
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
                type = "DECIMAL(" + (rdColumn.precision() > 0?rdColumn.precision():15) + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATETIME";
            }else if(info.getColumnType() == ColumnType.DATE){
                type = "DATE";
            }else if(info.getColumnType() == ColumnType.TIME){
                type = "TIME";
            }else if(info.getColumnType() == ColumnType.YEAR){
                type = "YEAR";
            }else{
                throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
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
            throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }

    @Override
    public String getColumnType(TableColumn column) {
        String type = "";
        if("TEXT".equalsIgnoreCase(column.getType())
                || "MEDIUMTEXT".equalsIgnoreCase(column.getType())
                || "LONGTEXT".equalsIgnoreCase(column.getType())
                || "BLOB".equalsIgnoreCase(column.getType())
                || "VARBINARY".equalsIgnoreCase(column.getType())
                || "TIMESTAMP".equalsIgnoreCase(column.getType())
                || "DATETIME".equalsIgnoreCase(column.getType())
                || "YEAR".equalsIgnoreCase(column.getType())
                || "DATE".equalsIgnoreCase(column.getType())
                || "TIME".equalsIgnoreCase(column.getType())
                || "DOUBLE".equalsIgnoreCase(column.getType())
                || "REAL".equalsIgnoreCase(column.getType())
                || "FLOAT".equalsIgnoreCase(column.getType())
                ){
            return column.getType().toUpperCase(Locale.ROOT);
        } else if("CHAR".equalsIgnoreCase(column.getType()) || "VARCHAR".equalsIgnoreCase(column.getType()) || "VARBINARY".equalsIgnoreCase(column.getType())){
            type =  column.getType().toUpperCase(Locale.ROOT) + "(" + column.getLength() + ")";
        } else if("INT".equalsIgnoreCase(column.getType()) || "BIGINT".equalsIgnoreCase(column.getType())){
            type =  column.getType().toUpperCase(Locale.ROOT) + "(" + column.getPrecision() + ")";
        } else if("DECIMAL".equalsIgnoreCase(column.getType())){
            type = "DECIMAL(" + column.getPrecision() + "," + column.getScale() + ")";
        } else{
            type = getColumnTypeByClassName(column);
        }
        return type;
    }

    private String getColumnTypeByClassName(TableColumn column) {
        String type = null;
        String className = column.getColumnClass();
        if(String.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.TEXT){
                type = "TEXT";
            }else if(column.getColumnType() == ColumnType.BLOB){
                type = "BLOB";
            }else if(column.getColumnType() == ColumnType.CLOB) {
                type = "TEXT";
            }else if(column.getColumnType() == ColumnType.BINARY){
                type = "VARBINARY(" + column.getLength() + ")";
            }else if(column.getColumnType() == ColumnType.CHAR){
                type = "CHAR(" + column.getLength() + ")";
            }else{
                type = "VARCHAR(" + column.getLength() + ")";
            }
        }else if (Integer.class.getName().equals(className)) {
            type = "INT(" + column.getPrecision() + ")";
        }else if(Date.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.LONG){
                type = "DECIMAL(" + column.getPrecision() + ", 0)";
            }else if(column.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(column.getColumnType() == ColumnType.DATETIME){
                type = "DATETIME";
            }else{
                throw new ORMException("Not support type : " + className + "," + column.getColumnType());
            }
        }else if(Long.class.getName().equals(className)){
            type = "BIGINT(" + column.getPrecision() + ")";
        }else if(Double.class.getName().equals(className)){
            type = "DOUBLE";
        }else if(Float.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.REAL){
                type = "REAL";
            }else {
                type = "FLOAT";
            }
        }else if(BigDecimal.class.getName().equals(className)){
            type = "DECIMAL(" + column.getPrecision() + "," + column.getScale() + ")";
        }else if(byte[].class.getName().equals(className)){
            type = "VARBINARY(" + column.getLength() + ")";
        }else{
            throw new ORMException("Not support type : " + column.getColumnClass() + "," + column.getColumnType());
        }
        return type;
    }

    @Override
    public String getClassName(TableColumn column) {
        if("TEXT".equalsIgnoreCase(column.getType())
                ||"MEDIUMTEXT".equalsIgnoreCase(column.getType())
                ||"LONGTEXT".equalsIgnoreCase(column.getType())
                ||"BLOB".equalsIgnoreCase(column.getType())
                ||"CLOB".equalsIgnoreCase(column.getType())
                ||"VARBINARY".equalsIgnoreCase(column.getType())
                ||"CHAR".equalsIgnoreCase(column.getType())
                ||"VARCHAR".equalsIgnoreCase(column.getType())){
            return String.class.getName();
        }else if("INT".equalsIgnoreCase(column.getType())){
            return Integer.class.getName();
        }else if("TIMESTAMP".equalsIgnoreCase(column.getType())
                ||"DATETIME".equalsIgnoreCase(column.getType())
                ||"DATE".equalsIgnoreCase(column.getType())
                ||"TIME".equalsIgnoreCase(column.getType())
                ||"YEAR".equalsIgnoreCase(column.getType())){
            return Date.class.getName();
        }else if("BIGINT".equalsIgnoreCase(column.getType())){
            return Long.class.getName();
        }else if("DOUBLE".equalsIgnoreCase(column.getType())){
            return Double.class.getName();
        }else if("FLOAT".equalsIgnoreCase(column.getType())
                ||"REAL".equalsIgnoreCase(column.getType())){
            return Float.class.getName();
        }else if("DECIMAL".equalsIgnoreCase(column.getType())){
            if(column.getScale() != null && column.getScale().intValue() == 0){
//                if(column.getPrecision() != null && column.getPrecision().intValue() == 15){
//                    return Date.class.getName();
//                }else
                if(column.getPrecision() != null && (column.getPrecision().intValue() < 18 && column.getPrecision().intValue() >= 10)){
                    return Long.class.getName();
                }else if(column.getPrecision() != null && (column.getPrecision().intValue() <= 9)){
                    return Integer.class.getName();
                }else{
                    return BigDecimal.class.getName();
                }
            }
            return Double.class.getName();
        }else{
            throw new ORMException("Not support type : " + column.getColumn() + "," + column.getType());
        }
    }
}
