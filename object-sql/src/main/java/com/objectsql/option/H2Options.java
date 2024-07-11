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

import com.objectsql.annotation.*;
import com.objectsql.exception.ORMException;
import com.objectsql.support.Table;
import com.objectsql.support.TableColumn;
import com.objectsql.utils.ORMUtils;
import com.objectsql.support.ColumnInfo;
import com.objectsql.support.ColumnType;

import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class H2Options extends MySQLOptions{
    @Override
    public String keyword() {
        return "h2";
    }

    @Override
    public String databaseType() {
        return "H2";
    }

    @Override
    public List<Table> tables(Connection connection, String keyword) {
        List<Table> temp = new ArrayList<Table>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT TABLE_NAME,REMARKS FROM information_schema.TABLES WHERE TABLE_CATALOG = ? ";
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

    @Override
    public Table table(Connection connection, RdTable rdTable) throws ORMException{
        String tableName = getTableName(rdTable);
        Table table = new Table(tableName);
        table.setSensitive(rdTable.sensitive());
        return table(connection, table);
    }

    @Override
    public Table table(Connection connection, Table table){
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String tableName = getCaseSensitive(table.getName(), table.getSensitive());
            String dbName = connection.getCatalog();
            String sql = "SELECT TABLE_NAME,REMARKS,TABLE_CATALOG FROM information_schema.TABLES WHERE TABLE_NAME = ? AND TABLE_CATALOG = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            if(rs.next()) {
                return new Table(rs.getString(1), rs.getString(2));
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

    private void print(Connection connection){
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT TABLE_NAME,REMARKS,TABLE_CATALOG FROM information_schema.TABLES WHERE TABLE_CATALOG = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, dbName);
            rs = ps.executeQuery();
            while(rs.next()) {
                System.out.println(rs.getString(1) + "--->" +  rs.getString(2));
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
    }

    @Override
    public boolean tableExists(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.TABLES WHERE TABLE_NAME = ? AND TABLE_CATALOG = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            if(rs.next()) {
                return true;
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
        return false;
    }

    @Override
    public List<TableColumn> columns(Connection connection, String tableName){
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.COLUMNS WHERE TABLE_NAME = ? AND TABLE_CATALOG = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            ps.setString(2, dbName);
            rs = ps.executeQuery();
            while (rs.next()){
                TableColumn column = new TableColumn(tableName, rs.getString("COLUMN_NAME"));
                column.setType(rs.getString("TYPE_NAME").toUpperCase(Locale.ROOT));
                column.setLength(rs.getLong("CHARACTER_MAXIMUM_LENGTH"));
                column.setNullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                column.setPrecision(rs.getInt("NUMERIC_PRECISION"));
                column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
                column.setScale(rs.getInt("NUMERIC_SCALE"));
                column.setOrder(rs.getInt("ORDINAL_POSITION"));
                column.setComment(rs.getString("REMARKS"));
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
    public List<TableColumn> columns(Connection connection, RdTable rdTable) throws ORMException {
        String tableName = getTableName(rdTable);
        return columns(connection, tableName);
    }

    private List<String> tableConstraints(Connection connection){
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<String>();
        try {
            String sql = "SELECT CONSTRAINT_NAME AS INDEX_NAME FROM INFORMATION_SCHEMA.CONSTRAINTS";
            ps = connection.prepareStatement(sql);
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
    public List<String> createOrUpdateSqls(Connection connection, RdTable table, List<ColumnInfo> infos, boolean tableExisted, List<TableColumn> tableColumns){
        String tableName = getCaseSensitive(table.name(), table.sensitive());
        List<String> sqls = new ArrayList<String>();
        List<String> constraints = tableConstraints(connection);
        if(table.dropped()){
            if(tableExisted){
                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                    sqls.add(String.format("DROP TABLE %s", tableName));
                }else{
                    sqls.add(String.format("DROP TABLE \"%s\"", tableName));
                }
            }
        }else{
            //create table
            if(tableExisted){
                Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
                for (TableColumn column : tableColumns){
                    columnMap.put(column.getColumn(), column);
                }
                // add or drop, next version modify.
                for(ColumnInfo info : infos){
                    String columnName =  getCaseSensitive(info.getColumnName(), table.sensitive());
                    TableColumn tableColumn = columnMap.get(columnName);
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" DROP COLUMN \"%s\"", tableName, columnName));
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
                                    needUpdate = true;
                                }
                            }
                            if(!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())){
                                String defaultValue = tableColumn.getDefaultValue().trim();
                                if(defaultValue.startsWith("'") && defaultValue.endsWith("'")){
                                    defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                                }
                                if(!defaultValue.equals(rdColumn.defaultValue())){
                                    needUpdate = true;
                                }
                            }
                            RdId rdId = info.getField().getAnnotation(RdId.class);
                            if (!needUpdate && (tableColumn.isNullable() != rdColumn.nullable()) && !tableColumn.isPrimaryKey() && rdId == null){
                                String temp = columnStringChangeNull(info, table.sensitive(), rdColumn.nullable());
                                sqls.add(String.format("ALTER TABLE %s ALTER %s ", tableName, temp));
                            }
                            if(needUpdate) {
                                String temp = columnString(info, table.sensitive(), rdColumn, false);

                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s", tableName, temp));
                                }else{
                                    sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN %s", tableName, temp));
                                }
                                String comment = columnComment(rdColumn);
                                if (!ORMUtils.isEmpty(comment)) {
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName,
                                                comment));
                                    }else{
                                        sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName,
                                                comment));
                                    }
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){

                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, table.sensitive(), rdColumn, true);
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s ADD COLUMN %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" ADD COLUMN %s", tableName, temp));
                            }
                            String comment = columnComment(rdColumn);
                            if (!ORMUtils.isEmpty(comment)) {
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName,
                                            comment));
                                }else{
                                    sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName,
                                            comment));
                                }
                            }
                        }
                    }
                    if(!info.getPrimaryKey() && uniqueKey != null) {
                        if(!constraints.contains(uniqueKey.name().toUpperCase(Locale.ROOT))){
                            String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                            if(uniqueSQL != null) {
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add("ALTER TABLE " + tableName + " ADD " + uniqueSQL);
                                }else{
                                    sqls.add("ALTER TABLE \"" + tableName + "\" ADD " + uniqueSQL);
                                }
                            }
                        }
                    }
                    if(foreignKey != null){
                        if(!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))){
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add("ALTER TABLE " + tableName + " ADD " + foreignSQL);
                                }else{
                                    sqls.add("ALTER TABLE \"" + tableName + "\" ADD " + foreignSQL);
                                }
                            }
                        }
                    }
                }
            }else{
                StringBuffer sql = new StringBuffer();
                sql.append("CREATE TABLE ");
                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                    sql.append(tableName);
                }else{
                    sql.append(String.format("\"%s\" ", tableName));
                }
                sql.append(" (");
                List<String> columnSQL = new ArrayList<String>();
                List<String> comments = new ArrayList<String>();
                for(int i = 0; i < infos.size(); i++){
                    ColumnInfo info = infos.get(i);
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    String temp = columnString(info, table.sensitive(), rdColumn, true);
                    String comment = columnComment(rdColumn);
                    String columnName =  getCaseSensitive(info.getColumnName(), table.sensitive());
                    if(!ORMUtils.isEmpty(comment)){
                        if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                            comments.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName,
                                    comment));
                        }else{
                            comments.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName,
                                    comment));
                        }
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
                    if(foreignKey != null){
                        if(!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))){
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                columnSQL.add(foreignSQL);
                            }
                        }
                    }
                }
                sql.append(ORMUtils.join(columnSQL, ","));
                sql.append(")");
                String comment = table.comment();
                sqls.add(sql.toString());
                if(!ORMUtils.isEmpty(comment)){
                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                        sqls.add(String.format("COMMENT ON TABLE %s IS '%s'", tableName, comment));
                    }else{
                        sqls.add(String.format("COMMENT ON TABLE \"%s\" IS '%s'", tableName, comment));
                    }
                }
                sqls.addAll(comments);
            }
        }
        return sqls;
    }

    @Override
    public List<String> createOrUpdateSqls(Connection connection, Table table, List<TableColumn> columns, List<TableColumn> tableColumns, boolean tableExisted) {
        String tableName = getCaseSensitive(table.getName(), table.getSensitive());

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
            sqls.add(sql.toString());
            String comment = table.getComment();
            if(!ORMUtils.isEmpty(comment)){
                sqls.add("COMMENT ON TABLE " + tableName + " IS '" + comment + "';");
            }
        }
        return sqls;
    }

    protected String columnStringChangeNull(ColumnInfo info, int sensitive, boolean isNull) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(info.getColumnName(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("\"%s\"", cname));
        }
        if(isNull) {
            temp.append(" SET NULL");
        }else{
            temp.append(" SET NOT NULL");
        }
        return temp.toString();
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
            temp.append(String.format("\"%s\"", cname));
        }
        temp.append(" ");
        String type = getColumnType(info, rdColumn);
        if(!ORMUtils.isEmpty(rdColumn.defaultValue())){
            type += " DEFAULT '" +  rdColumn.defaultValue() + "'";
        }
        if(!rdColumn.nullable() && addKey){
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
            if(info.getColumnType() == ColumnType.TEXT ||
                    info.getColumnType() == ColumnType.MEDIUM_TEXT ||
                    info.getColumnType() == ColumnType.LONG_TEXT){
                type = "TEXT";
            }else if(info.getColumnType() == ColumnType.BLOB){
                type = "BLOB";
            }else if(info.getColumnType() == ColumnType.CLOB) {
                type = "CLOB";
            }else if(info.getColumnType() == ColumnType.BINARY){
                type = "VARBINARY(" + rdColumn.length() + ")";
            }else if(info.getColumnType() == ColumnType.CHAR){
                type = "CHAR(" + rdColumn.length() + ")";
            }else{
                type = "VARCHAR(" + rdColumn.length()  + ")";
            }
        }else if (Integer.class.getName().equals(infoType)) {
            type = "INTEGER";
        }else if(Date.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.LONG){
                type = "DECIMAL(" + (rdColumn.precision() > 0?rdColumn.precision():15) + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATE";
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
                type = "DOUBLE";
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
}
