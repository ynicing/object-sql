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

import com.ursful.framework.orm.annotation.*;
import com.ursful.framework.orm.exception.ORMException;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

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
        return table(connection, tableName, rdTable.name(), rdTable.sensitive() == RdTable.DEFAULT_SENSITIVE);
    }

    @Override
    public Table table(Connection connection, String tableName){
        return table(connection, tableName, tableName, false);
    }

    private Table table(Connection connection, String tableName, String originalTableName, boolean sensitive){
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        try {
            String sql = "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  relname = ? ";
//            "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  (relname = ? OR relname = ?) ";
//            "where relname in (select tablename from pg_tables where schemaname='public' and position('_2' in tablename)=0 AND (TABLE_NAME = ? OR TABLE_NAME = ?) ) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
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
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
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
            throw new RuntimeException(e);
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
                        tableColumn.setLength(Integer.parseInt(stype[0]));
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
                        tableColumn.setLength(Integer.parseInt(stype[0]));
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
            return name.toLowerCase(Locale.ROOT);//默认
        }
    }

    @Override
    public List<String> createOrUpdateSqls(Connection connection, RdTable table, List<ColumnInfo> infos, boolean tableExisted, List<TableColumn> tableColumns) {
        List<String> sqls = new ArrayList<String>();
        String tableName = getCaseSensitive(table.name(), table.sensitive());
        if(table.dropped()){
            if(tableExisted){
                if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
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
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            if(table.sensitive() == RdTable.DEFAULT_SENSITIVE) {
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
                                    String typeString = getColumnType(info, rdColumn);
                                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                        sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, columnName, typeString));
                                    }else{
                                        sqls.add(String.format(" ALTER TABLE \"%s\" ALTER COLUMN \"%s\" TYPE %s", tableName, columnName, typeString));
                                    }
                                }
                            }
                            String defaultValue = tableColumn.getDefaultValue();
                            if(!StringUtils.isEmpty(defaultValue) && !StringUtils.isEmpty(rdColumn.defaultValue())){
                                if(!defaultValue.equals(rdColumn.defaultValue())){
                                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                        sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'", tableName, columnName, rdColumn.defaultValue()));
                                    }else{
                                        sqls.add(String.format(" ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET DEFAULT '%s'", tableName, columnName, rdColumn.defaultValue()));
                                    }
                                }
                            }
                            if(needUpdate) {
                                if(!rdColumn.nullable()){
                                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, columnName));
                                    }else{
                                        sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" SET NOT NULL", tableName, columnName));
                                    }
                                }else{
                                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", tableName, columnName));
                                    }else{
                                        sqls.add(String.format("ALTER TABLE \"%s\" ALTER COLUMN \"%s\" DROP NOT NULL", tableName, columnName));
                                    }
                                }
                                if (!StringUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                        sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                                    }else{
                                        sqls.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
                                    }
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                            RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, table.sensitive(), rdColumn, true);
                            if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" ADD %s", tableName, temp));
                            }
                            if(!info.getPrimaryKey()) {
                                String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                                if(uniqueSQL != null) {
                                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                        sqls.add("ALTER TABLE " + tableName + " ADD " + uniqueSQL);
                                    }else{
                                        sqls.add("ALTER TABLE \"" + tableName + "\" ADD " + uniqueSQL);
                                    }
                                }
                            }
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                                    sqls.add("ALTER TABLE " + tableName + " ADD " + foreignSQL);
                                }else{
                                    sqls.add("ALTER TABLE \"" + tableName + "\" ADD " + foreignSQL);
                                }
                            }

                            if (!StringUtils.isEmpty(comment)) {
                                if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
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
                if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
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
                    if(!StringUtils.isEmpty(comment)){
                        if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
                            comments.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                        }else{
                            comments.add(String.format("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s'", tableName, columnName, comment));
                        }
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
                sql.append(";");
                sqls.add(sql.toString());
                String comment = table.comment();
                if(StringUtils.isEmpty(comment)){
                    comment = table.title();
                }
                if(!StringUtils.isEmpty(comment)){
                    if(table.sensitive() == RdTable.DEFAULT_SENSITIVE){
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

    protected String columnString(ColumnInfo info, int sensitive, RdColumn rdColumn, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(info.getColumnName(), sensitive);
        if(sensitive == RdTable.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("\"%s\"", cname));
        }
        RdId rdId = info.getField().getAnnotation(RdId.class);
        if(rdId != null && rdId.autoIncrement() && !StringUtils.isEmpty(rdId.sequence())){
            temp.append(" " + rdId.sequence());
        }else{
            String type = getColumnType(info, rdColumn);
            temp.append(" " + type);
        }
        if(!StringUtils.isEmpty(rdColumn.defaultValue())){
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


    @Override
    protected String getColumnType(ColumnInfo info, RdColumn rdColumn) {
        String type = "";
        String infoType = info.getField().getType().getName();
        if(String.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.TEXT){
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
                type = "NUMERIC(" + rdColumn.datePrecision() + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATE";
            }else{
                throw new RuntimeException("Not support type : " + infoType + "," + info.getColumnType().name());
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
            throw new RuntimeException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }
}
