package com.ursful.framework.orm.option;

import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
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
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException {
        if(type == DataType.STRING && obj != null){
            if(columnType == ColumnType.BLOB || columnType == ColumnType.CLOB) {
                try {
                    ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
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
    public Table table(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        try {
            String sql = "select relname as TABLE_NAME,cast(obj_description(relfilenode,'pg_class') as varchar) as COMMENT from pg_class c where  (relname = ? OR relname = ?) ";
//            "where relname in (select tablename from pg_tables where schemaname='public' and position('_2' in tablename)=0 AND (TABLE_NAME = ? OR TABLE_NAME = ?) ) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            if(rs.next()) {
                table = new Table();
                table.setName(rs.getString("TABLE_NAME"));
                table.setComment(rs.getString("COMMENT"));
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
    public List<TableColumn> columns(Connection connection, String tableName) {
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String schemaName = connection.getSchema();
            String sql = "select c.relname, ad.adsrc, a.attnum,a.attname,t.typname,SUBSTRING(format_type(a.atttypid,a.atttypmod) from '\\(.*\\)') as data_type, format_type(a.atttypid,a.atttypmod),d.description, a.attnotnull, a.*  from pg_attribute a\n" +
                    "left join pg_type t on a.atttypid=t.oid \n" +
                    "left join pg_class c on a.attrelid=c.oid \n" +
                    "left join pg_description d on d.objsubid=a.attnum and d.objoid=a.attrelid \n" +
                    "left join pg_attrdef  ad  on ad.adrelid = a.attrelid and ad.adnum = a.attnum \n" +
                    "where   a.attnum>0  and (c.relname = ? or c.relname = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
//            ps.setString(3, schemaName.toUpperCase(Locale.ROOT));
//            ps.setString(4, schemaName.toLowerCase(Locale.ROOT));
//            ps.setString(5, dbName.toUpperCase(Locale.ROOT));
//            ps.setString(6, dbName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            while (rs.next()){
                TableColumn tableColumn = new TableColumn(rs.getString("RELNAME").toUpperCase(Locale.ROOT), rs.getString("ATTNAME").toUpperCase(Locale.ROOT));
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
                    String columnName = rdColumn.name().toUpperCase(Locale.ROOT);
                    String comment = columnComment(rdColumn);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, rdColumn.name().toUpperCase(Locale.ROOT)));
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
                                    sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s TYPE %s", tableName, columnName, typeString));
                                }
                            }
                            String defaultValue = tableColumn.getDefaultValue();
                            if(!StringUtils.isEmpty(defaultValue) && !StringUtils.isEmpty(rdColumn.defaultValue())){
                                if(!defaultValue.equals(rdColumn.defaultValue())){
                                    sqls.add(String.format(" ALTER TABLE %s ALTER COLUMN %s SET DEFAULT '%s'", tableName, columnName, rdColumn.defaultValue()));
                                }
                            }
                            if(needUpdate) {
                                if(!rdColumn.nullable()){
                                    sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s SET NOT NULL", tableName, columnName));
                                }else{
                                    sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s DROP NOT NULL", tableName, columnName));
                                }
                                if (!StringUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, rdColumn, true);
                            sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));
                            if (rdColumn.unique() && !info.getPrimaryKey()) {
                                String uniqueSQL = getUniqueSQL(table, rdColumn);
                                sqls.add("ALTER TABLE " + tableName + " ADD " + uniqueSQL);
                            }
                            if (!StringUtils.isEmpty(comment)) {
                                sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
                            }
                        }
                    }
                }
            }else{
                StringBuffer sql = new StringBuffer();
                sql.append("CREATE TABLE " + tableName + "(");
                List<String> columnSQL = new ArrayList<String>();
                List<String> comments = new ArrayList<String>();
                for(int i = 0; i < infos.size(); i++){
                    ColumnInfo info = infos.get(i);
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    String temp = columnString(info, rdColumn, true);
                    String comment = columnComment(rdColumn);
                    String columnName = rdColumn.name().toUpperCase(Locale.ROOT);
                    if(!StringUtils.isEmpty(comment)){
                        comments.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, columnName, comment));
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
                sql.append(";");
                sqls.add(sql.toString());
                if(!StringUtils.isEmpty(table.title())){
                    sqls.add(String.format("COMMENT ON TABLE %s IS '%s'", tableName, table.title()));
                }
                sqls.addAll(comments);

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
                type = "BYTEA";
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
