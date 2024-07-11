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
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class PostgreSQLOptions extends MySQLOptions{
    @Override
    public String databaseType() {
        return "PostgreSQL";
    }

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Pair pair) throws SQLException {
        Object obj = pair.getValue();
        ColumnType columnType = pair.getColumnType();
        DataType type =  DataType.getDataType(pair.getType());
        if(type == DataType.STRING && obj != null){
            if(columnType == ColumnType.BLOB || columnType == ColumnType.CLOB) {
                try {
                    ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes(getCoding(pair))));
                    return true;
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    @Override
    public String keyword() {
        return "PostgreSQL";
    }

    @Override
    public String nanoTimeSQL() {
        return "SELECT NOW()";
    }

    @Override
    public Table table(Connection connection, RdTable rdTable) throws ORMException {
        String tableName = getTableName(rdTable);
        return table(connection, tableName, rdTable.name(), rdTable.sensitive() == Table.DEFAULT_SENSITIVE);
    }

    @Override
    public Table table(Connection connection, Table table){
        String otableName = table.getName();
        String tableName = getCaseSensitive(table.getName(), table.getSensitive());
        return table(connection, tableName, otableName, false);
    }

    private Table table(Connection connection, String tableName, String originalTableName, boolean sensitive){
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        try {
//            String sql = "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  (relname = ? or relname = ?) ";
//            "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  (relname = ? OR relname = ?) ";
//            "where relname in (select tablename from pg_tables where schemaname='public' and position('_2' in tablename)=0 AND (TABLE_NAME = ? OR TABLE_NAME = ?) ) ";
            String sql = "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where relname = ? ";
            ps = connection.prepareStatement(sql);
//            if(sensitive) {
                ps.setString(1, tableName);
//            }else {
//                ps.setString(1, tableName.toLowerCase(Locale.ROOT));
//            }
            rs = ps.executeQuery();
            if(rs.next()) {
                table = new Table();
                if(sensitive) {
                    table.setName(originalTableName);
                }else{
                    table.setName(tableName);
                }
                table.setComment(rs.getString(2));
            }
        } catch (SQLException e) {
            throw new ORMException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }
        return table;
    }

    @Override
    public List<Table> tables(Connection connection, String keyword) {
        List<Table> temp = new ArrayList<Table>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        try {
            String sql = "select relname,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c ";
//            String sql = "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c";
//            "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  (relname = ? OR relname = ?) ";
//            "where relname in (select tablename from pg_tables where schemaname='public' and position('_2' in tablename)=0 AND (TABLE_NAME = ? OR TABLE_NAME = ?) ) ";
            if(!ORMUtils.isEmpty(keyword)){
                sql +=  "where relname like ? ";
            }
            ps = connection.prepareStatement(sql);
            if(!ORMUtils.isEmpty(keyword)){
                ps.setString(1, "%" + keyword + "%");
            }
            rs = ps.executeQuery();
            while(rs.next()) {
                temp.add(new Table(rs.getString(1), rs.getString(2)));
            }
        } catch (SQLException e) {
            throw new ORMException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }
        return temp;
    }

    @Override
    public boolean tableExists(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String sql = "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  relname = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            if(rs.next()) {
                return true;
            }
        } catch (SQLException e) {
            throw new ORMException(e);
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                }
            }
        }
        return false;
    }

    @Override
    public List<TableColumn> columns(Connection connection, RdTable rdTable) throws ORMException{
        String tableName = getTableName(rdTable);
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String primaryKey = null;
        try {
            // 查询列的时候，是大写就是大写，小写就是小写
            String sql = String.format("select a.attname from " +
                    "pg_constraint ct inner join pg_class c " +
                    "on ct.conrelid = c.oid " +
                    "inner join pg_attribute a on a.attrelid = c.oid " +
                    "and a.attnum = ct.conkey[1] " +
                    "inner join pg_type t on t.oid = a.atttypid " +
                    "where  ct.contype='p' and c.relname = '%s'", tableName);
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()){
                primaryKey = rs.getString(1);
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
        try {
            String dbName = connection.getCatalog();
            String schemaName = connection.getSchema();
            String sql = "select c.relname, ad.adsrc, a.attnum,a.attname,t.typname,SUBSTRING(format_type(a.atttypid,a.atttypmod) from '\\(.*\\)') as data_type, format_type(a.atttypid,a.atttypmod),d.description, a.attnotnull, a.*  from pg_attribute a " +
                    "left join pg_type t on a.atttypid=t.oid " +
                    "left join pg_class c on a.attrelid=c.oid " +
                    "left join pg_description d on d.objsubid=a.attnum and d.objoid=a.attrelid " +
                    "left join pg_attrdef  ad  on ad.adrelid = a.attrelid and ad.adnum = a.attnum " +
                    "where   a.attnum>0  and c.relname = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                String cname = rs.getString("ATTNAME");
                if(cname.contains(".pg.dropped.")){
                    continue;
                }
                TableColumn tableColumn = new TableColumn(tableName, cname);
                tableColumn.setType(rs.getString("TYPNAME"));
                String type = rs.getString("DATA_TYPE");
                if(type != null && type.startsWith("(") && type.endsWith(")")) {
                    String [] stype = type.substring(1, type.length() - 1).split(",");
                    if(stype.length == 1){
                        tableColumn.setLength(Long.parseLong(stype[0]));
                    }else if(stype.length == 2){
                        tableColumn.setPrecision(Integer.parseInt(stype[0]));
                        tableColumn.setScale(Integer.parseInt(stype[1]));
                    }
                }
                tableColumn.setNullable("f".equalsIgnoreCase(rs.getString("ATTNOTNULL")));
                tableColumn.setOrder(rs.getInt("ATTNUM"));
                tableColumn.setDefaultValue(rs.getString("ADSRC"));
                tableColumn.setComment(rs.getString("DESCRIPTION"));
                tableColumn.setIsPrimaryKey(tableColumn.getColumn().equalsIgnoreCase(primaryKey));
                columns.add(tableColumn);
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
    public List<TableColumn> columns(Connection connection, String tableName){
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String primaryKey = null;
        try {
            // 查询列的时候，是大写就是大写，小写就是小写
            String sql = String.format("select a.attname from " +
                    "pg_constraint ct inner join pg_class c " +
                    "on ct.conrelid = c.oid " +
                    "inner join pg_attribute a on a.attrelid = c.oid " +
                    "and a.attnum = ct.conkey[1] " +
                    "inner join pg_type t on t.oid = a.atttypid " +
                    "where  ct.contype='p' and (c.relname = '%s' or c.relname = '%s')", tableName.toLowerCase(Locale.ROOT), tableName.toUpperCase(Locale.ROOT));
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if (rs.next()){
                primaryKey = rs.getString(1);
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
        try {
            String dbName = connection.getCatalog();
            String schemaName = connection.getSchema();
            String sql = "select c.relname, ad.adsrc, a.attnum,a.attname,t.typname,SUBSTRING(format_type(a.atttypid,a.atttypmod) from '\\(.*\\)') as data_type, format_type(a.atttypid,a.atttypmod),d.description, a.attnotnull, a.*  from pg_attribute a " +
                    "left join pg_type t on a.atttypid=t.oid " +
                    "left join pg_class c on a.attrelid=c.oid " +
                    "left join pg_description d on d.objsubid=a.attnum and d.objoid=a.attrelid " +
                    "left join pg_attrdef  ad  on ad.adrelid = a.attrelid and ad.adnum = a.attnum " +
                    "where   a.attnum>0  and (c.relname = ? or c.relname = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            while (rs.next()){
                String cname = rs.getString("ATTNAME");
                if(cname.contains(".pg.dropped.")){
                    continue;
                }
                TableColumn tableColumn = new TableColumn(tableName, cname);
                tableColumn.setType(rs.getString("TYPNAME"));
                String type = rs.getString("DATA_TYPE");
                if(type != null && type.startsWith("(") && type.endsWith(")")) {
                    String [] stype = type.substring(1, type.length() - 1).split(",");
                    if(stype.length == 1){
                        tableColumn.setLength(Long.parseLong(stype[0]));
                    }else if(stype.length == 2){
                        tableColumn.setPrecision(Integer.parseInt(stype[0]));
                        tableColumn.setScale(Integer.parseInt(stype[1]));
                    }
                }
                tableColumn.setNullable("f".equalsIgnoreCase(rs.getString("ATTNOTNULL")));
                tableColumn.setOrder(rs.getInt("ATTNUM"));
                tableColumn.setDefaultValue(rs.getString("ADSRC"));
                tableColumn.setComment(rs.getString("DESCRIPTION"));
                tableColumn.setIsPrimaryKey(tableColumn.getColumn().equalsIgnoreCase(primaryKey));
                columns.add(tableColumn);
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

    private List<String> tableConstraints(Connection connection){
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<String>();
        try {
            String sql = "SELECT CONNAME AS INDEX_NAME FROM PG_CONSTRAINT WHERE CONTYPE = 'f' OR CONTYPE= 'u' OR CONTYPE= 'p'";
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
            return name.toLowerCase(Locale.ROOT);//默认
        }
    }

    @Override
    public List<String> createOrUpdateSqls(Connection connection, RdTable table, List<ColumnInfo> infos, boolean tableExisted, List<TableColumn> tableColumns) {
        List<String> sqls = new ArrayList<String>();
        String tableName = getCaseSensitive(table.name(), table.sensitive());

        List<String> constraints = tableConstraints(connection);

        if(table.dropped()){
            if(tableExisted){
                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                    sqls.add(String.format("DROP TABLE %s", tableName));
                }else {
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
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    String columnName =  getCaseSensitive(rdColumn.name(), table.sensitive());
                    TableColumn tableColumn = columnMap.get(columnName);
                    String comment = columnComment(rdColumn);
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
                                sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s ", tableName, columnName));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" DROP COLUMN \"%s\" ", tableName, columnName));
                            }
                        }else{

                            boolean needUpdate = false;
                            boolean isNumber = false;
                            if("NVARCHAR".equalsIgnoreCase(tableColumn.getType()) || "NCHAR".equalsIgnoreCase(tableColumn.getType())){
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
                                    needUpdate = true;
                                    String typeString = getColumnType(info, rdColumn);
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                        sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, columnName, typeString));
                                    }else{
                                        sqls.add(String.format(" ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s", tableName, columnName, typeString));
                                    }
                                }
                            }
                            String defaultValue = tableColumn.getDefaultValue();
                            if(!ORMUtils.isEmpty(defaultValue) && !ORMUtils.isEmpty(rdColumn.defaultValue())){
                                //PostgreSQL save  value as  '1::bigint'
                                String [] vals = defaultValue.split("::");
                                boolean update = false;
                                if (isNumber){
                                    update = new BigDecimal(tableColumn.getDefaultValue()).compareTo(
                                            new BigDecimal(rdColumn.defaultValue())) != 0;
                                }else{
                                    update = !vals[0].equals(rdColumn.defaultValue());
                                }
                                if(update){
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                        sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'", tableName, columnName, rdColumn.defaultValue()));
                                    }else{
                                        sqls.add(String.format(" ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET DEFAULT '%s'", tableName, columnName, rdColumn.defaultValue()));
                                    }
                                }
                            }
                            RdId rdId = info.getField().getAnnotation(RdId.class);
                            if (!needUpdate && (tableColumn.isNullable() != rdColumn.nullable()) && !tableColumn.isPrimaryKey() && rdId == null){
                                needUpdate = true;
                            }
                            if(needUpdate) {
                                if(!rdColumn.nullable()){
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, columnName));
                                    }else{
                                        sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", tableName, columnName));
                                    }
                                }else{
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", tableName, columnName));
                                    }else{
                                        sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP NOT NULL", tableName, columnName));
                                    }
                                }
                                if (!ORMUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                                    }else{
                                        sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
                                    }
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, table.sensitive(), rdColumn, true);
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" ADD %s", tableName, temp));
                            }

                            if (!ORMUtils.isEmpty(comment)) {
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                                }else{
                                    sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
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
                    if (foreignKey != null){
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
                    String columnName = getCaseSensitive(rdColumn.name(), table.sensitive());
                    if(!ORMUtils.isEmpty(comment)){
                        if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                            comments.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                        }else{
                            comments.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
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
                    if (foreignKey != null){
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
                sql.append(";");
                sqls.add(sql.toString());
                String comment = table.comment();
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
    public String dropTable(Table table){
        if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
            return String.format("DROP TABLE %s", table.getName());
        }else {
            return String.format("DROP TABLE \"%s\"", table.getName());
        }
    }

    @Override
    public List<String> createOrUpdateSqls(Connection connection, Table table, List<TableColumn> columns, List<TableColumn> tableColumns, boolean tableExisted) {
        List<String> sqls = new ArrayList<String>();
        String tableName = getCaseSensitive(table.getName(), table.getSensitive());

        List<String> constraints = tableConstraints(connection);

        //create table
        if(tableExisted){
            Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
            for (TableColumn column : tableColumns){
                columnMap.put(column.getColumn(), column);
            }
            // add or drop, next version modify.
            for(TableColumn tc : columns){
                String columnName =  getCaseSensitive(tc.getColumn(), table.getSensitive());
                TableColumn tableColumn = columnMap.get(columnName);
                String comment = tc.getComment();
                if(tableColumn != null){
                    if(tc.isDropped()){
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE) {
                            sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s ", tableName, columnName));
                        }else{
                            sqls.add(String.format("ALTER TABLE \"%s\" DROP COLUMN \"%s\" ", tableName, columnName));
                        }
                    }else{

                        boolean needUpdate = false;
                        if("NVARCHAR".equalsIgnoreCase(tableColumn.getType()) || "NCHAR".equalsIgnoreCase(tableColumn.getType())){
                            if(tableColumn.getLength() == null){
                                continue;
                            }
                            if(tableColumn.getLength().intValue() != tc.getLength()){
                                needUpdate = true;
                            }
                        }else if("DECIMAL".equalsIgnoreCase(tableColumn.getType())){
                            if(tableColumn.getPrecision() == null || tableColumn.getScale() == null){
                                continue;
                            }
                            if("Date".equalsIgnoreCase(tc.getType())){
                                if ((tableColumn.getPrecision().intValue() != tc.getPrecision()) || (tableColumn.getScale().intValue() != 0)) {
                                    needUpdate = true;
                                }
                            }else {
                                if ((tableColumn.getPrecision().intValue() != tc.getPrecision()) || (tableColumn.getScale().intValue() != tc.getScale())) {
                                    needUpdate = true;
                                }
                            }
                        }else{
                            String type = getColumnType(tc).toUpperCase(Locale.ROOT);
                            if(!type.startsWith(tableColumn.getType().toUpperCase(Locale.ROOT))){
                                needUpdate = true;
                                String typeString = getColumnType(tc);
                                if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, columnName, typeString));
                                }else{
                                    sqls.add(String.format(" ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s", tableName, columnName, typeString));
                                }
                            }
                        }
                        String defaultValue = tableColumn.getDefaultValue();
                        if(!ORMUtils.isEmpty(defaultValue) && !ORMUtils.isEmpty(tc.getDefaultValue())){
                            if(!defaultValue.equals(tc.getDefaultValue())){
                                if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'", tableName, columnName, tc.getDefaultValue()));
                                }else{
                                    sqls.add(String.format(" ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET DEFAULT '%s'", tableName, columnName, tc.getDefaultValue()));
                                }
                            }
                        }
                        if (!needUpdate && (tableColumn.isNullable() != tc.isNullable()) && !tableColumn.isPrimaryKey() && !tc.isPrimaryKey()){
                            needUpdate = true;
                        }
                        if(needUpdate) {
                            if(!tc.isNullable()){
                                if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, columnName));
                                }else{
                                    sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", tableName, columnName));
                                }
                            }else{
                                if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", tableName, columnName));
                                }else{
                                    sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP NOT NULL", tableName, columnName));
                                }
                            }
                            if (!ORMUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                                }else{
                                    sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
                                }
                            }
                        }
                    }
                }else{
                    if(!tc.isDropped()){
                        // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                        String temp = columnString(tc, table.getSensitive(), true);
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                            sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));
                        }else{
                            sqls.add(String.format("ALTER TABLE \"%s\" ADD %s", tableName, temp));
                        }

                        if (!ORMUtils.isEmpty(comment)) {
                            if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                            }else{
                                sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
                            }
                        }
                    }
                }
            }
        }else{
            StringBuffer sql = new StringBuffer();
            sql.append("CREATE TABLE ");
            if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                sql.append(tableName);
            }else{
                sql.append(String.format("\"%s\" ", tableName));
            }
            sql.append(" (");
            List<String> columnSQL = new ArrayList<String>();
            List<String> comments = new ArrayList<String>();
            for(int i = 0; i < columns.size(); i++){
                TableColumn tc = columns.get(i);
                String temp = columnString(tc, table.getSensitive(), true);
                String comment = tc.getComment();
                String columnName = getCaseSensitive(tc.getColumn(), table.getSensitive());
                if(!ORMUtils.isEmpty(comment)){
                    if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                        comments.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                    }else{
                        comments.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
                    }
                }
                columnSQL.add(temp.toString());
            }
            sql.append(ORMUtils.join(columnSQL, ","));
            sql.append(")");
            sql.append(";");
            sqls.add(sql.toString());
            String comment = table.getComment();
            if(!ORMUtils.isEmpty(comment)){
                if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                    sqls.add(String.format("COMMENT ON TABLE %s IS '%s'", tableName, comment));
                }else{
                    sqls.add(String.format("COMMENT ON TABLE \"%s\" IS '%s'", tableName, comment));
                }
            }
            sqls.addAll(comments);

        }
        return sqls;
    }

    protected String columnString(ColumnInfo info, int sensitive, RdColumn rdColumn, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(info.getColumnName(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("\"%s\"", cname));
        }
        RdId rdId = info.getField().getAnnotation(RdId.class);
        if(rdId != null && rdId.autoIncrement() && !ORMUtils.isEmpty(rdId.sequence())){
            temp.append(" " + rdId.sequence());
        }else{
            String type = getColumnType(info, rdColumn);
            temp.append(" " + type);
        }
        if(!ORMUtils.isEmpty(rdColumn.defaultValue())){
            temp.append(" DEFAULT '" +  rdColumn.defaultValue() + "'");
        }
        if(!rdColumn.nullable()){
            temp.append(" NOT NULL");
        }
        if(info.getPrimaryKey() && addKey){
            temp.append(" PRIMARY KEY");
        }
        return temp.toString();
    }

    protected String columnString(TableColumn tc, int sensitive, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(tc.getColumn(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("\"%s\"", cname));
        }
        if(tc.isPrimaryKey()  &&  tc.isAutoIncrement() && !ORMUtils.isEmpty(tc.getSequence())){
            temp.append(" " + tc.getSequence());
        }else{
            String type = getColumnType(tc);
            temp.append(" " + type);
        }
        if(!ORMUtils.isEmpty(tc.getDefaultValue())){
            temp.append(" DEFAULT '" +  tc.getDefaultValue() + "'");
        }
        if(!tc.isNullable()){
            temp.append(" NOT NULL");
        }
        if(tc.isPrimaryKey() && addKey){
            temp.append(" PRIMARY KEY");
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
                type = "BYTEA";
            }else if(info.getColumnType() == ColumnType.CLOB) {
                type = "BYTEA";
            }else if(info.getColumnType() == ColumnType.BINARY){
                type = "BYTEA";
            }else if(info.getColumnType() == ColumnType.CHAR){
                type = "BPCHAR(" + rdColumn.length() + ")";
            }else{
                type = "VARCHAR(" + rdColumn.length()  + ")";
            }
        }else if (Integer.class.getName().equals(infoType)) {
            type = "INT4";
        }else if(java.util.Date.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.LONG){
                type = "NUMERIC(" + (rdColumn.precision() > 0?rdColumn.precision():15) + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME || info.getColumnType() == ColumnType.DATE){
                type = "DATE";
            }else{
                throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
            }
        }else if(Long.class.getName().equals(infoType)){
            type = "INT8";
        }else if(Double.class.getName().equals(infoType)){
            type = "NUMERIC(" + rdColumn.precision() + "," + rdColumn.scale() + ")";
        }else if(Float.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.REAL){
                type = "float4";
            }else {
                type = "FLOAT8";
            }
        }else if(BigDecimal.class.getName().equals(infoType)){
            type = "NUMERIC(" + rdColumn.precision() + "," + rdColumn.scale() + ")";
        }else if(byte[].class.getName().equals(infoType)){
            type = "BYTEA";
        }else{
            throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }

    @Override
    public String getColumnType(TableColumn column) {
        String type = "";
        if("TEXT".equalsIgnoreCase(column.getType())
                || "BYTEA".equalsIgnoreCase(column.getType())
                || "INT4".equalsIgnoreCase(column.getType())
                || "TIMESTAMP".equalsIgnoreCase(column.getType())
                || "DATE".equalsIgnoreCase(column.getType())
                || "INT8".equalsIgnoreCase(column.getType())
                || "FLOAT8".equalsIgnoreCase(column.getType())
                || "FLOAT4".equalsIgnoreCase(column.getType())
                ){
            return column.getType().toUpperCase(Locale.ROOT);
        } else if("BPCHAR".equalsIgnoreCase(column.getType()) || "VARCHAR".equalsIgnoreCase(column.getType())){
            type =  column.getType().toUpperCase(Locale.ROOT) + "(" + column.getLength() + ")";
        } else if("NUMERIC".equalsIgnoreCase(column.getType())){
            type = "NUMERIC(" + column.getPrecision() + "," + column.getScale() + ")";
        } else{
            type = getColumnTypeByClassName(column);
        }
        return type;
    }

    private String getColumnTypeByClassName(TableColumn column) {
        String type = "";
        String className = column.getColumnClass();
        if(String.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.TEXT){
                type = "TEXT";
            }else if(column.getColumnType() == ColumnType.BLOB){
                type = "BYTEA";
            }else if(column.getColumnType() == ColumnType.CLOB) {
                type = "BYTEA";
            }else if(column.getColumnType() == ColumnType.BINARY){
                type = "BYTEA";
            }else if(column.getColumnType() == ColumnType.CHAR){
                type = "BPCHAR(" + column.getLength() + ")";
            }else{
                type = "VARCHAR(" + column.getLength()  + ")";
            }
        }else if (Integer.class.getName().equals(className)) {
            type = "INT4";
        }else if(java.util.Date.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.LONG){
                type = "NUMERIC(" + column.getPrecision() + ", 0)";
            }else if(column.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(column.getColumnType() == ColumnType.DATETIME){
                type = "DATE";
            }else{
                throw new ORMException("Not support type : " + className + "," + column.getColumnType());
            }
        }else if(Long.class.getName().equals(className)){
            type = "INT8";
        }else if(Double.class.getName().equals(className)){
            type = "NUMERIC(" + column.getPrecision() + "," + column.getScale() + ")";
        }else if(Float.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.REAL){
                type = "float4";
            }else {
                type = "FLOAT8";
            }
        }else if(BigDecimal.class.getName().equals(className)){
            type = "NUMERIC(" + column.getPrecision() + "," + column.getScale() + ")";
        }else if(byte[].class.getName().equals(className)){
            type = "BYTEA";
        }else{
            throw new ORMException("Not support type : " + className + "," + column.getColumnType());
        }
        return type;
    }

    @Override
    public String getClassName(TableColumn column) {
        if("TEXT".equalsIgnoreCase(column.getType())
                ||"BYTEA".equalsIgnoreCase(column.getType())
                ||"BPCHAR".equalsIgnoreCase(column.getType())
                ||"VARCHAR".equalsIgnoreCase(column.getType())){
            return String.class.getName();
        }else if("INT4".equalsIgnoreCase(column.getType())){
            return Integer.class.getName();
        }else if("INT8".equalsIgnoreCase(column.getType())){
            return Long.class.getName();
        }else if("FLOAT4".equalsIgnoreCase(column.getType())
                ||"FLOAT8".equalsIgnoreCase(column.getType())){
            return Float.class.getName();
        }else if("TIMESTAMP".equalsIgnoreCase(column.getType())
                ||"DATE".equalsIgnoreCase(column.getType())){
            return Date.class.getName();
        }else if("NUMERIC".equalsIgnoreCase(column.getType())){
            if(column.getScale() != null && column.getScale().intValue() == 0){
                if(column.getPrecision() != null && (column.getPrecision().intValue() <= 9)){
                    return Integer.class.getName();
                }else if(column.getPrecision() != null && (column.getPrecision().intValue() < 18 && column.getPrecision().intValue() >= 10)){
                    return Long.class.getName();
                }else {
                    return BigDecimal.class.getName();
                }
            }
            return Double.class.getName();
        }else{
            throw new ORMException("Not support type : " + column.getColumn() + "," + column.getType());
        }
    }
}
