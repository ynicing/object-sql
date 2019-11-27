package com.ursful.framework.orm.option;

import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.support.ColumnInfo;
import com.ursful.framework.orm.support.ColumnType;
import com.ursful.framework.orm.support.Table;
import com.ursful.framework.orm.support.TableColumn;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.StringUtils;

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
    public Table table(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.TABLES WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) AND (TABLE_CATALOG = ? OR TABLE_CATALOG = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            ps.setString(3, dbName.toUpperCase(Locale.ROOT));
            ps.setString(4, dbName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            if(rs.next()) {
                return new Table(rs.getString("TABLE_NAME"), rs.getString("REMARKS"));
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
    public boolean tableExists(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.TABLES WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) AND (TABLE_CATALOG = ? OR TABLE_CATALOG = ?)";
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
    public List<TableColumn> columns(Connection connection, String tableName) {
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String dbName = connection.getCatalog();
            String sql = "SELECT * FROM information_schema.COLUMNS WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) AND (TABLE_CATALOG = ? OR TABLE_CATALOG = ?)";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            ps.setString(3, dbName.toUpperCase(Locale.ROOT));
            ps.setString(4, dbName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            while (rs.next()){
                TableColumn column = new TableColumn(rs.getString("TABLE_NAME").toUpperCase(Locale.ROOT), rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
                column.setType(rs.getString("TYPE_NAME").toUpperCase(Locale.ROOT));
                column.setLength(rs.getInt("CHARACTER_MAXIMUM_LENGTH"));
                column.setNullable("YES".equalsIgnoreCase(rs.getString("IS_NULLABLE")));
                column.setPrecision(rs.getInt("NUMERIC_PRECISION"));
                column.setDefaultValue(rs.getString("COLUMN_DEFAULT"));
                column.setScale(rs.getInt("NUMERIC_SCALE"));
                column.setOrder(rs.getInt("ORDINAL_POSITION"));
                column.setComment(rs.getString("REMARKS"));
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
                                String defaultValue = tableColumn.getDefaultValue().trim();
                                if(defaultValue.startsWith("'") && defaultValue.endsWith("'")){
                                    defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                                }
                                if(!defaultValue.equals(rdColumn.defaultValue())){
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
                                temp += " COMMENT '" + comment + "'";
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
                String comment = table.comment();
                if(StringUtils.isEmpty(comment)){
                    comment = table.title();
                }
                if(!StringUtils.isEmpty(comment)){
                    sql.append(" COMMENT='" + comment + "' ");
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
                type = "DECIMAL(" + rdColumn.datePrecision() + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "TIMESTAMP";
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
                type = "DOUBLE";
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
