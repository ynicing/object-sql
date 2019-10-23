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
import java.sql.Date;
import java.util.*;

public class SQLServerOptions extends AbstractOptions{

    @Override
    public String keyword() {
            return "server";
    }

    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException {
        if((type == DataType.DATE) && (columnType != ColumnType.LONG) && (columnType != ColumnType.DATETIME) && (obj != null)){
            ps.setTimestamp(i + 1, null);
            return true;
        }
        return false;
    }

    @Override
    public String getColumnWithOperator(OperatorType operatorType, String name, String value) {
        String result = null;
        switch (operatorType){
            case XOR: //(x + y) - BITAND(x, y)*2
                result = "(" + name + "+" + value + " - 2*(" + name + "&" + value + "))";
                break;
            case LL: //x* power(2,y)
                result = "(" + name + "*POWER(2," + value + "))";
                break;
            case RR: //FLOOR(x/ power(2,y))
                result = "FLOOR(" + name + "/POWER(2," + value + ")";
                break;
        }
        return result;
    }

    @Override
    public String getColumnWithOperatorAndFunction(String function, boolean inFunction, OperatorType operatorType, String name, String value) {
        String result = null;
        switch (operatorType){
            case XOR: //(x + y) - BITAND(x, y)*2
                if(inFunction) {
                    result = function + "(" + name + "+" + value + " - 2*(" + name + "&" + value + "))";
                }else{
                    result = "(" + function +  "(" + name + ")+" + value + " - 2*(" +
                            function + "(" + name + ")&" + value + "))";
                }
                break;
            case LL: //x* power(2,y)
                if(inFunction) {
                    result = function + "(" + name + "*POWER(2," + value + "))";
                }else{
                    result = "(" + function + "(" + name + ")*POWER(2," + value + "))";
                }
                break;
            case RR: //FLOOR(x/ power(2,y))
                if(inFunction) {
                    result = function + "(FLOOR(" + name + "/POWER(2," + value + "))";
                }else{
                    result = "FLOOR(" + function + "(" + name + ")/POWER(2," + value + ")";
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
        return "SQLServer";
    }

    @Override
    public String nanoTimeSQL() {
        return "SELECT GETDATE()";
    }

    @Override
    public QueryInfo doQuery(IQuery query, Pageable page) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        sb.append(selectColumns(query, null));
        if(page != null){
            String byOrders = orders(query, null);
            if(!StringUtils.isEmpty(byOrders)){
                sb.append(" ,row_number() over(" + byOrders + ") rn_ ");
            }else{
                sb.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        sb.append(joins(query, values));
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        if(page != null){
            String tempSQL = sb.toString();
            sb = new StringBuffer("SELECT TOP " + page.getSize() +" * ");
            sb.append(" FROM (");
            sb.append(tempSQL);
            sb.append(") ms_ ");
            sb.append(" WHERE ms_.rn_ > " +page.getOffset()+" ");
        }else{
            sb.append(orders(query, null));
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
        if(start != null && size != null){
            String byOrders = null;
            if(multiOrder != null) {
                String orders = QueryUtils.getOrders(this, multiOrder.getOrders());
                if (!StringUtils.isEmpty(orders)) {
                    byOrders = " ORDER BY " + orders;
                }
            }
            if(byOrders != null){
                sql.append(" ,row_number() over(order by " + byOrders + ") rn_ ");
            }else{
                sql.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }
        sql.append(" FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(terms != null) {
            Condition condition = terms.getCondition();
            String conditions = QueryUtils.getConditions(this, clazz, ORMUtils.newList(condition), values);
            if (!StringUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }
        if(size != null && size.intValue() > 0){
            if(start == null){
                start = 0;
            }
            String tempSQL = sql.toString();
            sql = new StringBuffer("SELECT TOP " + size +" * ");
            sql.append(" FROM (");
            sql.append(tempSQL);
            sql.append(") ms_ ");
            sql.append(" WHERE ms_.rn_ > " + ((Math.max(1, start) - 1) * size)+" ");
        }else{
            String byOrders = null;
            if(multiOrder != null) {
                String orders = QueryUtils.getOrders(this, multiOrder.getOrders());
                if (!StringUtils.isEmpty(orders)) {
                    byOrders = " ORDER BY " + orders;
                }
            }
            if(byOrders != null){
                sql.append(byOrders);
            }
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;
    }


    @Override
    public Table table(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        try {
            String schemaName = connection.getSchema();
            String sql = String.format("select * from %s.sysobjects where (id = object_id('%s') or id = object_id('%s')) and type = 'U'", schemaName,tableName.toUpperCase(Locale.ROOT),tableName.toLowerCase(Locale.ROOT));
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if(rs.next()){
                table = new Table(rs.getString("NAME"));
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
        if(table != null) {
            try {
                String schemaName = connection.getSchema();
                String sql = String.format(" SELECT o.name, p.value FROM sys.extended_properties p " +
                        "        LEFT JOIN sysobjects o ON p.major_id= o.id" +
                        "        WHERE  p.minor_id=0 and (o.name= '%s' OR o.name= '%s')", schemaName, tableName.toUpperCase(Locale.ROOT), tableName.toLowerCase(Locale.ROOT));
                ps = connection.prepareStatement(sql);
                rs = ps.executeQuery();
                if(rs.next()){
                    table.setComment(rs.getString("VALUE"));
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
        }
        return table;
    }

    @Override
    public List<TableColumn> columns(Connection connection, String tableName) {
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
//            String dbName = connection.getCatalog();
            String schemaName = connection.getSchema();
            String sql = String.format("select g.value AS COMMENT,e.text as VALUE,b.name AS TYPE, a.colorder, a.name,a.prec, a.scale, a.isnullable, a.xprec, a.xscale from dbo.syscolumns a " +
                    "left join sys.extended_properties g on a.id=g.major_id and a.colid=g.minor_id " +
                    "left join syscomments e on a.cdefault=e.id " +
                    "left join systypes b on a.xusertype=b.xusertype " +
                    "where (a.id = object_id('%s') or a.id = object_id('%s')) ", schemaName,tableName.toUpperCase(Locale.ROOT),tableName.toLowerCase(Locale.ROOT));
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()){
                TableColumn tableColumn = new TableColumn(tableName.toUpperCase(Locale.ROOT), rs.getString("NAME").toUpperCase(Locale.ROOT));
                tableColumn.setType(rs.getString("TYPE"));
                tableColumn.setLength(rs.getInt("PREC"));
                tableColumn.setPrecision(rs.getInt("XPREC"));
                tableColumn.setScale(rs.getInt("XSCALE"));
                tableColumn.setNullable("1".equalsIgnoreCase(rs.getString("ISNULLABLE")));
                tableColumn.setOrder(rs.getInt("COLORDER"));
                tableColumn.setDefaultValue(rs.getString("VALUE"));
                tableColumn.setComment(rs.getString("COMMENT"));
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
                                }
                            }
                            if(!needUpdate && !StringUtils.isEmpty(tableColumn.getDefaultValue())){
                                String defaultValue = tableColumn.getDefaultValue();
                                if(defaultValue.startsWith("('") && defaultValue.endsWith("')")){
                                    defaultValue = defaultValue.substring(2, defaultValue.length() - 2);
                                }
                                if(!defaultValue.equals(rdColumn.defaultValue())){
                                    needUpdate = true;
                                }
                            }
                            if(needUpdate) {
                                String temp = columnString(info, rdColumn, false);
                                sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s", tableName, temp));
                                if (!StringUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                            comment, tableName, rdColumn.name().toUpperCase(Locale.ROOT)));
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
                                sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                        comment, tableName, rdColumn.name().toUpperCase(Locale.ROOT)));
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
                    if(!StringUtils.isEmpty(comment)){
                        comments.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                comment, tableName, rdColumn.name().toUpperCase(Locale.ROOT)));
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
                    sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',NULL,NULL",
                            table.title(), tableName));
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
                type = "TEXT";
            }else if(info.getColumnType() == ColumnType.BLOB){
                type = "IMAGE";
            }else if(info.getColumnType() == ColumnType.CLOB) {
                type = "TEXT";
            }else if(info.getColumnType() == ColumnType.BINARY){
                type = "VARBINARY(" + rdColumn.length() + ")";
            }else if(info.getColumnType() == ColumnType.CHAR){
                type = "NCHAR(" + rdColumn.length() + ")";
            }else{
                type = "NVARCHAR(" + rdColumn.length()  + ")";
            }
        }else if (Integer.class.getName().equals(infoType)) {
            type = "INT";
        }else if(java.util.Date.class.getName().equals(infoType)){
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
            type = "BIGINT";
        }else if(Double.class.getName().equals(infoType)){
            type = "DECIMAL(" + rdColumn.precision() + "," + rdColumn.scale() + ")";
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
