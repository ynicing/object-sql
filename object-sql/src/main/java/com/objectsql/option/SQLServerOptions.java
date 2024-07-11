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

public class SQLServerOptions extends AbstractOptions{

    @Override
    public String keyword() {
            return "server";
    }

    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Pair pair) throws SQLException {
        Object obj = pair.getValue();
        ColumnType columnType = pair.getColumnType();
        DataType type =  DataType.getDataType(pair.getType());
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

    private List<String> tableConstraints(Connection connection){
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<String>();
        try {
//            String sql = " SELECT IDX.NAME AS INDEX_NAME FROM SYS.INDEXES IDX JOIN SYS.INDEX_COLUMNS IDXCOL ON (IDX.OBJECT_ID = IDXCOL.OBJECT_ID AND IDX.INDEX_ID = IDXCOL.INDEX_ID) " +
//                    "JOIN SYS.TABLES TAB ON (IDX.OBJECT_ID = TAB.OBJECT_ID)";
            String sql = "SELECT NAME AS INDEX_NAME FROM SYS.ALL_OBJECTS WHERE TYPE='UQ' OR TYPE='F' OR TYPE='PK'";
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
    public QueryInfo doQuery(IQuery query, Pageable page) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        Map<String, String> asNames = new HashMap<String, String>();
        sb.append(selectColumns(query, null, asNames, values));
        String group = groups(query, null);
        if(page != null && ORMUtils.isEmpty(group)){
            String byOrders = orders(query, null, asNames);
            if(!ORMUtils.isEmpty(byOrders)){
                sb.append(" ,row_number() over(" + byOrders + ") rn_ ");
            }else{
                sb.append(" ,row_number() over(order by (select 0)) rn_ ");
            }
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        if(query instanceof IMultiQuery) {
            sb.append(joins((IMultiQuery)query, values));
        }
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        if(page != null){
            String tempSQL = sb.toString();
            sb = new StringBuffer("SELECT TOP " + page.getSize() +" * ");
            sb.append(" FROM (");
            if(ORMUtils.isEmpty(group)) {
                sb.append(tempSQL);
            }else{
                sb.append("SELECT sqlserver_.*, ROW_NUMBER() OVER(ORDER BY (SELECT 0)) RN_ FROM " +
                        "(" + tempSQL +  ") as sqlserver_ ");
            }
            sb.append(") ms_ ");
            sb.append(" WHERE ms_.rn_ > " +page.getOffset()+" ");
        }else{
            sb.append(orders(query, null, asNames));
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
        if(start != null && size != null){
            String byOrders = null;
            if(multiOrder != null) {
                String orders = getOrders(multiOrder.getOrders(), clazz);
                if (!ORMUtils.isEmpty(orders)) {
                    byOrders = orders;
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
        if(condition != null) {
            String conditions = getConditions(clazz, ORMUtils.newList(condition), values);
            if (!ORMUtils.isEmpty(conditions)) {
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
                String orders = getOrders(multiOrder.getOrders(), clazz);
                if (!ORMUtils.isEmpty(orders)) {
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
    public boolean tableExists(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String schemaName = connection.getSchema();
            //sysobjects 必须小写
            String sql = String.format("select * from %s.sysobjects where id = object_id('%s') and type = 'U'", schemaName, tableName);
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if(rs.next()){
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
    public Table table(Connection connection, RdTable rdTable) throws ORMException{
        String tableName = getTableName(rdTable);
        Table table = new Table(tableName);
        table.setSensitive(rdTable.sensitive());
        return table(connection, table);
    }

    @Override
    public Table table(Connection connection, Table tab){
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        String tableName = getCaseSensitive(tab.getName(), tab.getSensitive());
        try {
            String schemaName = connection.getSchema();
            //写入大写就是大写，小写就是小写
            //不区分大小写同样（区别于查询条件，如 TULIP 与 tulip等价
            String sql = String.format("select name from %s.sysobjects where id = object_id('%s') and type = 'U'", schemaName, tableName);
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            if(rs.next()){
                table = new Table(rs.getString(1));
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
        if(table != null) {
            try {
                String sql = String.format(" SELECT o.name, p.value FROM sys.extended_properties p " +
                        "        LEFT JOIN sysobjects o ON p.major_id= o.id" +
                        "        WHERE  p.minor_id=0 and o.name= '%s'", tableName);
                ps = connection.prepareStatement(sql);
                rs = ps.executeQuery();
                if(rs.next()){
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
        }
        return table;
    }

    @Override
    public List<TableColumn> columns(Connection connection, RdTable rdTable) {
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        return columns(connection, tableName);
    }

    @Override
    public List<TableColumn> columns(Connection connection, String tableName) {
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        String primaryKey = null;
        try {
            // 查询列的时候，是大写就是大写，小写就是小写
            String sql = String.format("SELECT colm.name FROM sys.columns colm, sys.indexes idx, sys.index_columns ic " +
                    "where idx.index_id = ic.index_id and colm.column_id  = ic.column_id  " +
                    "and idx.object_id = ic.object_id and colm.object_id = idx.object_id " +
                    "and idx.is_primary_key = 1  and idx.object_id = OBJECT_ID('%s') ", tableName);
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
            // 查询列的时候，是大写就是大写，小写就是小写
            String schemaName = connection.getSchema();
            String sql = String.format("select g.value AS COMMENT,e.text as VALUE,b.name AS TYPE, a.colorder, a.name,a.prec, a.scale, a.isnullable, a.xprec, a.xscale from dbo.syscolumns a " +
                    "left join sys.extended_properties g on a.id=g.major_id and a.colid=g.minor_id " +
                    "left join syscomments e on a.cdefault=e.id " +
                    "left join systypes b on a.xusertype=b.xusertype " +
                    "where a.id = object_id('%s') ", tableName);
            ps = connection.prepareStatement(sql);
            rs = ps.executeQuery();
            while (rs.next()){
                TableColumn tableColumn = new TableColumn(tableName, rs.getString("NAME"));
                tableColumn.setType(rs.getString("TYPE"));
                tableColumn.setLength(rs.getLong("PREC"));
                tableColumn.setPrecision(rs.getInt("XPREC"));
                tableColumn.setScale(rs.getInt("XSCALE"));
                tableColumn.setNullable("1".equalsIgnoreCase(rs.getString("ISNULLABLE")));
                tableColumn.setOrder(rs.getInt("COLORDER"));
                tableColumn.setDefaultValue(rs.getString("VALUE"));
                tableColumn.setComment(rs.getString("COMMENT"));
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
    public List<String> createOrUpdateSqls(Connection connection, RdTable table, List<ColumnInfo> infos, boolean tableExisted, List<TableColumn> tableColumns) {
        String tableName = getCaseSensitive(table.name(), table.sensitive());
        List<String> sqls = new ArrayList<String>();
        List<String> constraints = tableConstraints(connection);
        if(table.dropped()){
            if(tableExisted){
                sqls.add(String.format("DROP TABLE %s", tableName));
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
                    String columnName = getCaseSensitive(info.getColumnName(), table.sensitive());
                    TableColumn tableColumn = columnMap.get(columnName);
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    String comment = columnComment(rdColumn);
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
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
                                }
                            }
                            if(!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())){
                                String defaultValue = tableColumn.getDefaultValue();
                                if(defaultValue.startsWith("('") && defaultValue.endsWith("')")){
                                    defaultValue = defaultValue.substring(2, defaultValue.length() - 2);
                                }
                                if (isNumber){
                                    if (ORMUtils.isEmpty(rdColumn.defaultValue())){
                                        needUpdate = true;
                                    }else{
                                        needUpdate = new BigDecimal(tableColumn.getDefaultValue()).compareTo(
                                                new BigDecimal(rdColumn.defaultValue())) != 0;
                                    }
                                }else {
                                    if(!defaultValue.equals(rdColumn.defaultValue())){
                                        needUpdate = true;
                                    }
                                }
                            }
                            RdId rdId = info.getField().getAnnotation(RdId.class);
                            if (!needUpdate && (tableColumn.isNullable() != rdColumn.nullable()) && !tableColumn.isPrimaryKey() && rdId == null){
                                needUpdate = true;
                            }
                            if(needUpdate) {
                                String temp = columnString(info, table.sensitive(), rdColumn, false);
                                sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s", tableName, temp));
                                if (!ORMUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    // MS_Description
                                    sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                            comment, tableName, columnName));
                                }
                            }
                        }
                    }else{
                        if(!rdColumn.dropped()){
                            // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                            String temp = columnString(info, table.sensitive(), rdColumn, true);
                            sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));

                            if (!ORMUtils.isEmpty(comment)) {
                                //MS_Description
                                sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                        comment, tableName, columnName));
                            }
                        }
                    }
                    if(!info.getPrimaryKey() && uniqueKey != null) {
                        if(!constraints.contains(uniqueKey.name().toUpperCase(Locale.ROOT))){
                            String uniqueSQL = getUniqueSQL(table, rdColumn, uniqueKey);
                            if(uniqueSQL != null) {
                                sqls.add("ALTER TABLE " + tableName + " ADD " + uniqueSQL);
                            }
                        }
                    }
                    if (foreignKey != null){
                        if(!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))){
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                sqls.add("ALTER TABLE " + tableName + " ADD " + foreignSQL);
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
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    String temp = columnString(info, table.sensitive(), rdColumn, true);
                    String comment = columnComment(rdColumn);
                    String columnName = getCaseSensitive(rdColumn.name(), table.sensitive());
                    if(!ORMUtils.isEmpty(comment)){
                        comments.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                comment, tableName, columnName));
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
                    sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',NULL,NULL",
                            comment, tableName));
                }
                sqls.addAll(comments);

            }
        }
        return sqls;
    }

    @Override
    public String dropTable(Table table){
        return String.format("DROP TABLE %s", table.getName());
    }

    public List<String> createOrUpdateSqls(Connection connection, Table table, List<TableColumn> columns, List<TableColumn> tableColumns, boolean tableExisted) {
        String tableName = getCaseSensitive(table.getName(), table.getSensitive());
        List<String> sqls = new ArrayList<String>();
        List<String> constraints = tableConstraints(connection);

        //create table
        if(tableExisted){
            Map<String, TableColumn> columnMap = new HashMap<String, TableColumn>();
            for (TableColumn column : tableColumns){
                columnMap.put(column.getColumn(), column);
            }
            // add or drop, next version modify.
            for(TableColumn tc : columns){
                String columnName = getCaseSensitive(tc.getColumn(), table.getSensitive());
                TableColumn tableColumn = columnMap.get(columnName);
                String comment = tc.getComment();
                if(tableColumn != null){
                    if(tc.isDropped()){
                        sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
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
                            }
                        }
                        if(!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())){
                            String defaultValue = tableColumn.getDefaultValue();
                            if(defaultValue.startsWith("('") && defaultValue.endsWith("')")){
                                defaultValue = defaultValue.substring(2, defaultValue.length() - 2);
                            }
                            if(!defaultValue.equals(tc.getDefaultValue())){
                                needUpdate = true;
                            }
                        }
                        if (!needUpdate && (tableColumn.isNullable() != tc.isNullable()) && !tableColumn.isPrimaryKey() && !tc.isPrimaryKey()){
                            needUpdate = true;
                        }
                        if(needUpdate) {
                            String temp = columnString(tc, table.getSensitive(), false);
                            sqls.add(String.format("ALTER TABLE %s ALTER COLUMN %s", tableName, temp));
                            if (!ORMUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                // MS_Description
                                sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                        comment, tableName, columnName));
                            }
                        }
                    }
                }else{
                    if(!tc.isDropped()){
                        // int, bigint(忽略精度), decimal(精度）， varchar， char 判断长度， 其他判断类型，+ 默认值
                        String temp = columnString(tc, table.getSensitive(), true);
                        sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));

                        if (!ORMUtils.isEmpty(comment)) {
                            //MS_Description
                            sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                                    comment, tableName, columnName));
                        }
                    }
                }
            }
        }else{
            StringBuffer sql = new StringBuffer();
            sql.append("CREATE TABLE " + tableName + "(");
            List<String> columnSQL = new ArrayList<String>();
            List<String> comments = new ArrayList<String>();
            for(int i = 0; i < columns.size(); i++){
                TableColumn tc = columns.get(i);
                String temp = columnString(tc, table.getSensitive(), true);
                String comment = tc.getComment();
                String columnName = getCaseSensitive(tc.getColumn(), table.getSensitive());
                if(!ORMUtils.isEmpty(comment)){
                    comments.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',N'column',N'%s'",
                            comment, tableName, columnName));
                }
                columnSQL.add(temp.toString());
            }
            sql.append(ORMUtils.join(columnSQL, ","));
            sql.append(")");
            sql.append(";");
            sqls.add(sql.toString());
            String comment = table.getComment();
            if(!ORMUtils.isEmpty(comment)){
                sqls.add(String.format("EXECUTE sp_addextendedproperty N'MS_Description',N'%s',N'user',N'dbo',N'table',N'%s',NULL,NULL",
                        comment, tableName));
            }
            sqls.addAll(comments);

        }
        return sqls;
    }

    protected String columnString(ColumnInfo info, int sensitive, RdColumn rdColumn, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(info.getColumnName(), sensitive);
        temp.append(cname);
        String type = getColumnType(info, rdColumn);
        temp.append(" " + type);
        if(!ORMUtils.isEmpty(rdColumn.defaultValue())){
            temp.append(" DEFAULT '" +  rdColumn.defaultValue() + "'");
        }
        if(!rdColumn.nullable()){
            temp.append(" NOT NULL");
        }else{
            //与MySQL一样，默认not null
            if("timestamp".equalsIgnoreCase(type)){
                temp.append(" NULL");
            }
        }
        if(info.getPrimaryKey() && addKey){
            temp.append(" PRIMARY KEY");
        }
        RdId rdId = info.getField().getAnnotation(RdId.class);
        if(rdId != null && rdId.autoIncrement()){
            temp.append(" IDENTITY");
        }
        return temp.toString();
    }

    protected String columnString(TableColumn tc, int sensitive, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(tc.getColumn(), sensitive);
        temp.append(cname);
        String type = getColumnType(tc);
        temp.append(" " + type);
        if(!ORMUtils.isEmpty(tc.getDefaultValue())){
            temp.append(" DEFAULT '" +  tc.getDefaultValue() + "'");
        }
        if(!tc.isNullable()){
            temp.append(" NOT NULL");
        }else{
            //与MySQL一样，默认not null
            if("timestamp".equalsIgnoreCase(type)){
                temp.append(" NULL");
            }
        }
        if(tc.isPrimaryKey() && addKey){
            temp.append(" PRIMARY KEY");
        }
        if(tc.isPrimaryKey() && tc.isAutoIncrement()){
            temp.append(" IDENTITY");
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
                type = "DECIMAL(" + (rdColumn.precision() > 0?rdColumn.precision():15) + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATETIME";
            }else{
                throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
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
            throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }

    @Override
    public String getColumnType(TableColumn column) {
        String type = "";
        if("TEXT".equalsIgnoreCase(column.getType())
                || "IMAGE".equalsIgnoreCase(column.getType())
                || "INT".equalsIgnoreCase(column.getType())
                || "TIMESTAMP".equalsIgnoreCase(column.getType())
                || "DATETIME".equalsIgnoreCase(column.getType())
                || "BIGINT".equalsIgnoreCase(column.getType())
                || "REAL".equalsIgnoreCase(column.getType())
                || "FLOAT".equalsIgnoreCase(column.getType())
                ){
            return column.getType().toUpperCase(Locale.ROOT);
        } else if("NCHAR".equalsIgnoreCase(column.getType())
                || "CHAR".equalsIgnoreCase(column.getType())
                || "NVARCHAR".equalsIgnoreCase(column.getType())
                || "VARCHAR".equalsIgnoreCase(column.getType())
                || "VARBINARY".equalsIgnoreCase(column.getType())){
            type =  column.getType().toUpperCase(Locale.ROOT) + "(" + column.getLength() + ")";
        } else if("DECIMAL".equalsIgnoreCase(column.getType())){
            type = "DECIMAL(" + column.getPrecision() + "," + column.getScale() + ")";
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
                type = "IMAGE";
            }else if(column.getColumnType() == ColumnType.CLOB) {
                type = "TEXT";
            }else if(column.getColumnType() == ColumnType.BINARY){
                type = "VARBINARY(" + column.getLength() + ")";
            }else if(column.getColumnType() == ColumnType.CHAR){
                type = "NCHAR(" + column.getLength() + ")";
            }else{
                type = "NVARCHAR(" + column.getLength()  + ")";
            }
        }else if (Integer.class.getName().equals(className)) {
            type = "INT";
        }else if(java.util.Date.class.getName().equals(className)){
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
            type = "BIGINT";
        }else if(Double.class.getName().equals(className)){
            type = "DECIMAL(" + column.getPrecision() + "," + column.getScale() + ")";
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
            throw new ORMException("Not support type : " + className + "," + column.getColumnType());
        }
        return type;
    }

    @Override
    public String getClassName(TableColumn column) {
        if("TEXT".equalsIgnoreCase(column.getType())
                ||"IMAGE".equalsIgnoreCase(column.getType())
                ||"VARBINARY".equalsIgnoreCase(column.getType())
                ||"NCHAR".equalsIgnoreCase(column.getType())
                ||"NVARCHAR".equalsIgnoreCase(column.getType())
                ||"CHAR".equalsIgnoreCase(column.getType())
                ||"VARCHAR".equalsIgnoreCase(column.getType())){
            return String.class.getName();
        }else if("INT".equalsIgnoreCase(column.getType())){
            return Integer.class.getName();
        }else if("BIGINT".equalsIgnoreCase(column.getType())){
            return Long.class.getName();
        }else if("REAL".equalsIgnoreCase(column.getType())
                ||"FLOAT".equalsIgnoreCase(column.getType())){
            return Float.class.getName();
        }else if("TIMESTAMP".equalsIgnoreCase(column.getType())
                ||"DATETIME".equalsIgnoreCase(column.getType())){
            return Date.class.getName();
        }else if("DECIMAL".equalsIgnoreCase(column.getType())){
            if(column.getScale() != null && column.getScale().intValue() == 0){
                if(column.getPrecision() != null && (column.getPrecision().intValue() <= 9)){
                    return Integer.class.getName();
                }else if(column.getPrecision() != null && (column.getPrecision().intValue() < 18 && column.getPrecision().intValue() >= 10)){
                    return Long.class.getName();
                }else{
                    return BigDecimal.class.getName();
                }
            }
            return Double.class.getName();
        }else{
            throw new ORMException("Not support type : " + column.getColumn() + "," + column.getType());
        }
    }

    @Override
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
        Map<String, String> info = new HashMap<String, String>();
        try {
            String sql = " SELECT o.name, p.value FROM sys.extended_properties p " +
                    "        LEFT JOIN sysobjects o ON p.major_id= o.id" +
                    "        WHERE  p.minor_id=0 ";

            if(!ORMUtils.isEmpty(keyword)){
                sql +=  "and o.name like ? ";
            }
            ps = connection.prepareStatement(sql);
            if(!ORMUtils.isEmpty(keyword)){
                ps.setString(1, "%" + keyword + "%");
            }
            rs = ps.executeQuery();
            while(rs.next()){
                info.put(rs.getString(1), rs.getString(2));
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
        try {
            String schemaName = connection.getSchema();
            //写入大写就是大写，小写就是小写
            //不区分大小写同样（区别于查询条件，如 TULIP 与 tulip等价
            String sql = String.format("select name from %s.sysobjects where type = 'U' ", schemaName);
            if(!ORMUtils.isEmpty(keyword)){
                sql +=  "and name like ? ";
            }
            ps = connection.prepareStatement(sql);
            if(!ORMUtils.isEmpty(keyword)){
                ps.setString(1, "%" + keyword + "%");
            }
            rs = ps.executeQuery();
            while(rs.next()){
                String tableName = rs.getString(1);
                temp.add(new Table(tableName, info.get(tableName)));
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


}
