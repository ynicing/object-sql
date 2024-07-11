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
import com.objectsql.query.QueryUtils;
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class OracleOptions extends AbstractOptions{

    @Override
    public String keyword() {
        return "oracle";
    }

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i,Pair pair) throws SQLException {
        Object obj = pair.getValue();
        ColumnType columnType = pair.getColumnType();
        DataType type =  DataType.getDataType(pair.getType());
        boolean hasSet = false;
        switch (type) {
            case STRING:
                if(columnType == ColumnType.BLOB){
                    if(obj != null) {
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes(getCoding(pair))));
                            hasSet = true;
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }else{
                        ps.setBinaryStream(i + 1, null);
                    }
                }else if(columnType == ColumnType.CLOB){
                    try {
                        Clob oracleClob = (Clob) createOracleLob(connection, "oracle.sql.CLOB");
                        ps.setClob(i +1, oracleStr2Clob(obj.toString(), oracleClob));
                        hasSet = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                break;
            case DOUBLE:
//                BigDecimal bg=new BigDecimal((Double)obj);
//                bg.setScale(5,BigDecimal.ROUND_HALF_UP);
                ps.setDouble(i + 1, (Double)obj);
                hasSet = true;
                break;
            default:
        }
        return hasSet;
    }

    public static Object createOracleLob(Connection conn, String lobClassName)
            throws Exception {
        Class lobClass = conn.getClass().getClassLoader().loadClass(
                lobClassName);
        final Integer DURATION_SESSION = lobClass.getField("DURATION_SESSION").getInt(null);
        final Integer MODE_READWRITE = lobClass.getField("MODE_READWRITE").getInt(null);
        Method createTemporary = lobClass.getMethod("createTemporary", new Class[] { Connection.class, boolean.class, int.class });
        Object lob = createTemporary.invoke(null, new Object[] { conn, false, DURATION_SESSION });
        Method open = lobClass.getMethod("open", new Class[] { int.class });
        open.invoke(lob, new Object[] { MODE_READWRITE });
        return lob;
    }

    public static Clob oracleStr2Clob(String str, Clob lob) throws Exception {
        Method methodToInvoke = lob.getClass().getMethod(
                "getCharacterOutputStream", (Class[]) null);
        Writer writer = (Writer) methodToInvoke.invoke(lob, (Object[]) null);
        writer.write(str);
        writer.close();
        return lob;
    }

    @Override
    public String getColumnWithOperator(OperatorType operatorType, String name, String value) {
        String result = null;
        switch (operatorType){
            case AND://BITAND(x, y)
                result = "BITAND(" + name + "," + value + ")";
                break;
            case OR: //(x + y) - BITAND(x, y)
                result = "(" + name + "+" + value + " - BITAND(" + name + "," + value + "))";
                break;
            case XOR: //(x + y) - BITAND(x, y)*2
                result = "(" + name + "+" + value + " - 2*BITAND(" + name + "," + value + "))";
                break;
            case NOT: // (x -1 ) - BITAND(x, -1)*2
                result = "(" + name + "-1 - 2*BITAND(" + name + ", -1))";
                break;
            case LL: //x* power(2,y)
                result = "(" + name + "*POWER(2," + value + "))";
                break;
            case RR: //FLOOR(x/ power(2,y))
                result = "FLOOR(" + name + "/POWER(2," + value + ")";
                break;
            case MOD:
                result = "MOD(" + name + "," + value + ")";
                break;
        }
        return result;
    }

    @Override
    public String getColumnWithOperatorAndFunction(String function, boolean inFunction, OperatorType operatorType, String name, String value) {
        String result = null;
        switch (operatorType){
            case AND://BITAND(x, y)
                if(inFunction) {
                    result = function + "(BITAND(" + name + "," + value + "))";
                }else{
                    result = "BITAND(" + function + "(" + name + ")," + value + ")";
                }
                break;
            case OR: //(x + y) - BITAND(x, y)
                if(inFunction) {
                    result = function + "(" + name + "+" + value + " - BITAND(" + name + "," + value + "))";
                }else{
                    result = "(" + function +  "(" + name + ")+" + value + " - BITAND(" +
                            function + "(" + name + ")," + value + "))";
                }
                break;
            case XOR: //(x + y) - BITAND(x, y)*2
                if(inFunction) {
                    result = function + "(" + name + "+" + value + " - 2*BITAND(" + name + "," + value + "))";
                }else{
                    result = "(" + function +  "(" + name + ")+" + value + " - 2*BITAND(" +
                            function + "(" + name + ")," + value + "))";
                }
                break;
            case NOT: // (x -1 ) - BITAND(x, -1)*2
                if(inFunction) {
                    result = function + "(" + name + "-1 - 2*BITAND(" + name + ", -1))";
                }else{
                    result = "(" + function + "(" + name + ")-1 - 2*BITAND(" + function + "(" + name + "), -1))";
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
            case MOD:
                if(inFunction) {
                    result = function + "(MOD(" + name + "," + value + "))";
                }else{
                    result = "MOD(" + function + "(" + name + ")," + value + ")";
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
        return "Oracle";
    }

    @Override
    public String nanoTimeSQL() {
        return "SELECT SYSTIMESTAMP FROM DUAL";
    }

    private List<String> tableConstraints(Connection connection){
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> names = new ArrayList<String>();
        try {
            String sql = "SELECT CONSTRAINT_NAME AS INDEX_NAME FROM USER_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'R'  OR CONSTRAINT_TYPE = 'U' OR CONSTRAINT_TYPE = 'P'";
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

        String alias = "ora_";

        sb.append("SELECT ");
        Map<String, String> asNames = new HashMap<String, String>();
        String group = groups(query, null);
        if(!ORMUtils.isEmpty(group)){
            alias = null;
        }
        sb.append(selectColumns(query, alias, asNames, values));
        String order = orders(query, alias, asNames);
        boolean hasOrder = !ORMUtils.isEmpty(order);
        if(page != null && !hasOrder && ORMUtils.isEmpty(group)){
            sb.append(",ROWNUM rn_ ");
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, alias));
        if(query instanceof IMultiQuery) {
            sb.append(joins((IMultiQuery)query, values));
        }
        int now = sb.length();
        sb.append(wheres(query, values, null));
        int then = sb.length();
        boolean hasCondition = then > now;
        sb.append(group);
        sb.append(havings(query, values, null));
        sb.append(order);
        if(page != null){
            if(hasOrder || !ORMUtils.isEmpty(group)){
                sb = new StringBuffer("SELECT ora_a_.* FROM (SELECT ora_b_.*,ROWNUM rn_  FROM (" + sb.toString() + ") ora_b_ WHERE ROWNUM <= ?) ora_a_ WHERE ora_a_.rn_ > ? ");
            }else{
                sb = new StringBuffer("SELECT ora_a_.* FROM (" + sb.toString() + (hasCondition?" AND " : " WHERE ") + "ROWNUM <= ? ) ora_a_ WHERE ora_a_.rn_ > ? ");
            }
            values.add(new Pair(new Integer(page.getSize() + page.getOffset())));
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
        String alias = "ora_";
        if(names != null){
            String [] tempNames = new String[names.length];
            for(int i = 0; i < tempNames.length; i++){
                tempNames[i] = alias + "." + names[i];
            }
            names = tempNames;
        }
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT ");
        String nameStr = ORMUtils.join(names, ",");
        if(ORMUtils.isEmpty(nameStr)){
            sql.append(alias + "." + Column.ALL);
        }else{
            sql.append(nameStr);
        }
        boolean hasOrder = false;
        String order = "";
        if(multiOrder != null) {
            QueryUtils.setMultiOrderAlias(multiOrder, alias);
            String orders = getOrders(multiOrder.getOrders(), clazz);
            if (!ORMUtils.isEmpty(orders)) {
                hasOrder = true;
                order = " ORDER BY " + orders;
            }
        }
        if(start != null && size != null && !hasOrder){
            sql.append(",ROWNUM rn_ ");
        }
        sql.append(" FROM " + tableName + " " + alias);
        List<Pair> values = new ArrayList<Pair>();
        boolean hasCondition = false;

        if(condition != null) {
            QueryUtils.setConditionAlias(condition, alias);
            String conditions = getConditions(clazz, ORMUtils.newList(condition), values);
            if (!ORMUtils.isEmpty(conditions)) {
                sql.append(" WHERE " + conditions);
                hasCondition = true;
            }
        }
        sql.append(order);
        if(size != null && size.intValue() > 0){
            if(start == null){
                start = 0;
            }
            if(hasOrder){
                sql = new StringBuffer("SELECT ora_a_.* FROM (SELECT ora_b_.*,ROWNUM rn_ FROM (" + sql.toString() + ") ora_b_ WHERE ROWNUM <= ?) ora_a_ WHERE ora_a_.rn_ > ? ");
            }else{
                sql = new StringBuffer("SELECT ora_a_.* FROM (" + sql.toString() + (hasCondition?" AND " : " WHERE ") + " ROWNUM <= ? ) ora_a_ WHERE ora_a_.rn_ > ? ");
            }
            values.add(new Pair( ((Math.max(1, start)) * size)));
            values.add(new Pair( ((Math.max(1, start) - 1) * size)));
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
            String sql = "SELECT * FROM USER_TABLES WHERE  TABLE_NAME = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
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
            return name.toUpperCase(Locale.ROOT);//oracle默认大写
        }
    }

    @Override
    public List<Table> tables(Connection connection, String keyword) {
        List<Table> temp = new ArrayList<Table>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, String> info = new HashMap<String, String>();
        try {
            String sql = "SELECT TABLE_NAME,COMMENTS FROM USER_TAB_COMMENTS ";
            if(!ORMUtils.isEmpty(keyword)){
                sql +=  "WHERE TABLE_NAME LIKE ? ";
            }
            ps = connection.prepareStatement(sql);
            if(!ORMUtils.isEmpty(keyword)){
                ps.setString(1, "%" + keyword + "%");
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                info.put(rs.getString(1), rs.getString(2));
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
            //Oracle 将表名自动转为大写，所以查询只需要大写
            //但是为了兼容，做了小写查询
            String sql = "SELECT TABLE_NAME FROM USER_TABLES";
            if(!ORMUtils.isEmpty(keyword)){
                sql +=  "WHERE TABLE_NAME LIKE ? ";
            }
            ps = connection.prepareStatement(sql);
            if(!ORMUtils.isEmpty(keyword)){
                ps.setString(1, "%" + keyword + "%");
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                String tableName = rs.getString(1);
                temp.add(new Table(tableName, info.get(tableName)));//如果是小写同时被转为大写
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
    public Table table(Connection connection, RdTable rdTable)  throws ORMException{
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
            //Oracle 将表名自动转为大写，所以查询只需要大写
            //但是为了兼容，做了小写查询
            String sql = "SELECT TABLE_NAME FROM USER_TABLES WHERE  TABLE_NAME = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            if(rs.next()) {
                table = new Table();
                table.setName(rs.getString(1));//如果是小写同时被转为大写
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
        if(table != null){
            try {
                String sql = "SELECT COMMENTS FROM USER_TAB_COMMENTS WHERE  TABLE_NAME = ?";
                ps = connection.prepareStatement(sql);
                ps.setString(1, tableName);
                rs = ps.executeQuery();
                if(rs.next()) {
                    table.setComment(rs.getString(1));
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
        return table;
    }

    //oracle 如果规避大小写表名呢？ 又如何允许大小写区别呢？
    @Override
    public List<TableColumn> columns(Connection connection, RdTable rdTable) throws ORMException{
        String tableName = getTableName(rdTable);
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, String> columnComment = new HashMap<String, String>();
        try {
            String sql = "SELECT COLUMN_NAME,COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                //查询出来默认采用大写，为了下面comments能够匹配上
                String cname = rs.getString(1);
                if((rdTable.sensitive() == Table.DEFAULT_SENSITIVE) && (cname != null)){
                    cname = cname.toUpperCase(Locale.ROOT);
                }
                columnComment.put(cname, rs.getString(2));
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
            String sql = " SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE,COLUMN_ID,DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                String cname = rs.getString(2);
                if((rdTable.sensitive() == Table.DEFAULT_SENSITIVE) && (cname != null)){
                    cname = cname.toUpperCase(Locale.ROOT);
                }
                TableColumn tableColumn = new TableColumn(tableName, cname);
                tableColumn.setType(rs.getString("DATA_TYPE"));
                tableColumn.setLength(rs.getLong("DATA_LENGTH"));
                tableColumn.setPrecision(rs.getInt("DATA_PRECISION"));
                tableColumn.setScale(rs.getInt("DATA_SCALE"));
                tableColumn.setNullable("Y".equalsIgnoreCase(rs.getString("NULLABLE")));
                tableColumn.setOrder(rs.getInt("COLUMN_ID"));
                tableColumn.setDefaultValue(rs.getString("DATA_DEFAULT"));
                tableColumn.setComment(columnComment.get(tableColumn.getColumn()));
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

    public List<TableColumn> columns(Connection connection, String tableName){
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, String> columnComment = new HashMap<String, String>();
        try {
            String sql = "SELECT COLUMN_NAME,COMMENTS FROM USER_COL_COMMENTS WHERE TABLE_NAME = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                //查询出来默认采用大写，为了下面comments能够匹配上
                String cname = rs.getString(1);
                columnComment.put(cname, rs.getString(2));
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
        String primaryKey = null;
        try {
            String sql = "select col.column_name " +
                    "        from user_constraints con,  user_cons_columns col" +
                    "        where con.constraint_name = col.constraint_name" +
                    "        and con.constraint_type='P'" +
                    "        and col.table_name = ? ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            if (rs.next()){
                //查询出来默认采用大写，为了下面comments能够匹配上
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
            String sql = " SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE,DATA_LENGTH,DATA_PRECISION,DATA_SCALE,NULLABLE,COLUMN_ID,DATA_DEFAULT FROM USER_TAB_COLUMNS WHERE TABLE_NAME = ?";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName);
            rs = ps.executeQuery();
            while (rs.next()){
                String cname = rs.getString(2);
                TableColumn tableColumn = new TableColumn(tableName, cname);
                tableColumn.setType(rs.getString("DATA_TYPE"));
                tableColumn.setLength(rs.getLong("DATA_LENGTH"));
                tableColumn.setPrecision(rs.getInt("DATA_PRECISION"));
                tableColumn.setScale(rs.getInt("DATA_SCALE"));
                tableColumn.setNullable("Y".equalsIgnoreCase(rs.getString("NULLABLE")));
                tableColumn.setOrder(rs.getInt("COLUMN_ID"));
                tableColumn.setDefaultValue(rs.getString("DATA_DEFAULT"));
                tableColumn.setComment(columnComment.get(tableColumn.getColumn()));
                tableColumn.setIsPrimaryKey(cname.equalsIgnoreCase(primaryKey));
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
                    String columnName = getCaseSensitive(info.getColumnName(), table.sensitive());
                    TableColumn tableColumn = columnMap.get(columnName);
                    RdColumn rdColumn = info.getField().getAnnotation(RdColumn.class);
                    RdUniqueKey uniqueKey = info.getField().getAnnotation(RdUniqueKey.class);
                    RdForeignKey foreignKey = info.getField().getAnnotation(RdForeignKey.class);
                    String comment = columnComment(rdColumn);
                    if(tableColumn != null){
                        if(rdColumn.dropped()){
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
                                sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" DROP COLUMN \"%s\"", tableName, columnName));
                            }
                        }else{
                            boolean needUpdate = false;
                            boolean isNumber = false;
                            if("VARCHAR2".equalsIgnoreCase(tableColumn.getType()) || "CHAR".equalsIgnoreCase(tableColumn.getType())){
                                if(tableColumn.getLength() == null){
                                    continue;
                                }
                                if(tableColumn.getLength().intValue() != rdColumn.length()){
                                    needUpdate = true;
                                }
                            }else if("NUMBER".equalsIgnoreCase(tableColumn.getType())){
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
                                String columnType = tableColumn.getType().toUpperCase(Locale.ROOT);
                                int index = columnType.indexOf("(");
                                if(index != -1){
                                    columnType = columnType.substring(0, index);
                                }
                                //real --> float
                                if(!type.startsWith(columnType)){
                                    needUpdate = true;
                                }
                            }
                            if(!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())){
                                String defaultValue = tableColumn.getDefaultValue().trim();
                                if(defaultValue.startsWith("'") && defaultValue.startsWith("'")){
                                    defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                                }
                                if (isNumber){
                                    if (ORMUtils.isEmpty(rdColumn.defaultValue())){
                                        needUpdate = true;
                                    }else{
                                        needUpdate = new BigDecimal(tableColumn.getDefaultValue()).compareTo(
                                                new BigDecimal(rdColumn.defaultValue())) != 0;
                                    }
                                }else {
                                    if (!defaultValue.equals(rdColumn.defaultValue())) {
                                        needUpdate = true;
                                    }
                                }
                            }
                            RdId rdId = info.getField().getAnnotation(RdId.class);
                            if (!needUpdate && (tableColumn.isNullable() != rdColumn.nullable()) && !tableColumn.isPrimaryKey() && rdId == null){
                                String temp = columnStringChangeNull(info, table.sensitive(), rdColumn.nullable());
                                sqls.add(String.format("ALTER TABLE %s MODIFY %s", tableName, temp));
                            }
                            if(needUpdate) {
                                String temp = columnString(info, table.sensitive(), rdColumn, false);
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                    sqls.add(String.format("ALTER TABLE %s MODIFY %s", tableName, temp));
                                }else{
                                    sqls.add(String.format("ALTER TABLE \"%s\" MODIFY %s", tableName, temp));
                                }
                                if (!ORMUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
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
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
                                sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" ADD %s", tableName, temp));
                            }
                            if (!ORMUtils.isEmpty(comment)) {
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
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
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
                                    sqls.add("ALTER TABLE " + tableName + " ADD " + uniqueSQL);
                                }else{
                                    sqls.add("ALTER TABLE \"" + tableName + "\" ADD " + uniqueSQL);
                                }
                            }
                        }
                    }
                    if(foreignKey != null){
                        if(!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))) {
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                if(table.sensitive() == Table.DEFAULT_SENSITIVE) {
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
                    RdId rdId = info.getField().getAnnotation(RdId.class);
                    if(rdId != null && rdId.autoIncrement() && !ORMUtils.isEmpty(rdId.sequence())){
                        //todo
                        //create seq?
                        String seqName = getCaseSensitive(rdId.sequence(), table.sensitive());
                        PreparedStatement ps = null;
                        ResultSet rs = null;
                        boolean seqExist = false;
                        try {
                            ps = connection.prepareStatement("SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ? ");
                            ps.setString(1, seqName);
                            rs = ps.executeQuery();
                            if(rs.next()){
                                seqExist = true;
                            }
                            rs.close();
                            ps.close();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                        if(!seqExist){
                            if(table.sensitive() == Table.DEFAULT_SENSITIVE){
                                sql.append(String.format("CREATE SEQUENCE %s INCREMENT BY 1 START WITH 1", seqName));
                            }else{
                                sql.append(String.format("CREATE SEQUENCE \"%s\" INCREMENT BY 1 START WITH 1", seqName));
                            }
                        }
                    }
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
                    if(foreignKey != null){
                        if(!constraints.contains(foreignKey.name().toUpperCase(Locale.ROOT))) {
                            String foreignSQL = getForeignSQL(table, rdColumn, foreignKey);
                            if(foreignSQL != null) {
                                columnSQL.add(foreignSQL);
                            }
                        }
                    }
                }
                sql.append(ORMUtils.join(columnSQL, ","));
                sql.append(")");
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
                String columnName = getCaseSensitive(tc.getColumn(), table.getSensitive());
                TableColumn tableColumn = columnMap.get(columnName);
                String comment = tc.getComment();
                if(tableColumn != null){
                    if(tc.isDropped()){
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE) {
                            sqls.add(String.format("ALTER TABLE %s DROP COLUMN %s", tableName, columnName));
                        }else{
                            sqls.add(String.format("ALTER TABLE \"%s\" DROP COLUMN \"%s\"", tableName, columnName));
                        }
                    }else{
                        boolean needUpdate = false;
                        if("VARCHAR2".equalsIgnoreCase(tableColumn.getType()) || "CHAR".equalsIgnoreCase(tableColumn.getType())){
                            if(tableColumn.getLength() == null){
                                continue;
                            }
                            if(tableColumn.getLength().intValue() != tc.getLength()){
                                needUpdate = true;
                            }
                        }else if("NUMBER".equalsIgnoreCase(tableColumn.getType())){
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
                            String columnType = tableColumn.getType().toUpperCase(Locale.ROOT);
                            int index = columnType.indexOf("(");
                            if(index != -1){
                                columnType = columnType.substring(0, index);
                            }
                            //real --> float
                            if(!type.startsWith(columnType)){
                                needUpdate = true;
                            }
                        }
                        if(!needUpdate && !ORMUtils.isEmpty(tableColumn.getDefaultValue())){
                            String defaultValue = tableColumn.getDefaultValue().trim();
                            if(defaultValue.startsWith("'") && defaultValue.startsWith("'")){
                                defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                            }
                            if(!defaultValue.equals(tc.getDefaultValue())){
                                needUpdate = true;
                            }
                        }
                        if (!needUpdate && (tableColumn.isNullable() != tc.isNullable()) && !tableColumn.isPrimaryKey() && (!tc.isPrimaryKey())){
                            String temp = columnStringChangeNull(tc, table.getSensitive(), tc.isNullable());
                            sqls.add(String.format("ALTER TABLE %s MODIFY %s", tableName, temp));
                        }
                        if(needUpdate) {
                            String temp = columnString(tc, table.getSensitive(), false);
                            if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                                sqls.add(String.format("ALTER TABLE %s MODIFY %s", tableName, temp));
                            }else{
                                sqls.add(String.format("ALTER TABLE \"%s\" MODIFY %s", tableName, temp));
                            }
                            if (!ORMUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                if(table.getSensitive() == Table.DEFAULT_SENSITIVE) {
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
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE) {
                            sqls.add(String.format("ALTER TABLE %s ADD %s", tableName, temp));
                        }else{
                            sqls.add(String.format("ALTER TABLE \"%s\" ADD %s", tableName, temp));
                        }
                        if (!ORMUtils.isEmpty(comment)) {
                            if(table.getSensitive() == Table.DEFAULT_SENSITIVE) {
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
                if(tc.isPrimaryKey() && tc.isAutoIncrement() && !ORMUtils.isEmpty(tc.getSequence())){
                    //todo
                    //create seq?
                    String seqName = getCaseSensitive(tc.getSequence(), table.getSensitive());
                    PreparedStatement ps = null;
                    ResultSet rs = null;
                    boolean seqExist = false;
                    try {
                        ps = connection.prepareStatement("SELECT * FROM USER_SEQUENCES WHERE SEQUENCE_NAME = ? ");
                        ps.setString(1, seqName);
                        rs = ps.executeQuery();
                        if(rs.next()){
                            seqExist = true;
                        }
                        rs.close();
                        ps.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                    if(!seqExist){
                        if(table.getSensitive() == Table.DEFAULT_SENSITIVE){
                            sql.append(String.format("CREATE SEQUENCE %s INCREMENT BY 1 START WITH 1", seqName));
                        }else{
                            sql.append(String.format("CREATE SEQUENCE \"%s\" INCREMENT BY 1 START WITH 1", seqName));
                        }
                    }
                }
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

    protected String columnString(TableColumn tc, int sensitive, boolean addKey) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(tc.getColumn(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("\"%s\"", cname));
        }
        temp.append(" ");

        String type = getColumnType(tc);
        if(!ORMUtils.isEmpty(tc.getDefaultValue())){
            type += " DEFAULT '" +  tc.getDefaultValue() + "'";
        }
        if(!tc.isNullable() && addKey){
            type += " NOT NULL";
        }
        temp.append(type);
        if(tc.isPrimaryKey() && addKey){
            temp.append(" ");
            temp.append("PRIMARY KEY");
        }
        return temp.toString();
    }


    protected String columnStringChangeNull(TableColumn tc, int sensitive, boolean isNull) {
        StringBuffer temp = new StringBuffer();
        String cname = getCaseSensitive(tc.getColumn(), sensitive);
        if(sensitive == Table.DEFAULT_SENSITIVE){
            temp.append(cname);
        }else{
            temp.append(String.format("\"%s\"", cname));
        }
        if(isNull) {
            temp.append(" NULL");
        }else{
            temp.append(" NOT NULL");
        }
        return temp.toString();
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
            temp.append(" NULL");
        }else{
            temp.append(" NOT NULL");
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
                type = "CLOB";
            }else if(info.getColumnType() == ColumnType.BLOB){
                type = "BLOB";
            }else if(info.getColumnType() == ColumnType.CLOB) {
                type = "CLOB";
            }else if(info.getColumnType() == ColumnType.BINARY){
                type = "BLOB";
            }else if(info.getColumnType() == ColumnType.CHAR){
                type = "CHAR(" + rdColumn.length() + ")";
            }else{
                type = "VARCHAR2(" + rdColumn.length()  + ")";
            }
        }else if (Integer.class.getName().equals(infoType)) {
            type = "NUMBER(" + rdColumn.precision() + ")";
        }else if(java.util.Date.class.getName().equals(infoType)){
            if(info.getColumnType() == ColumnType.LONG){
                type = "NUMBER(" + (rdColumn.precision() > 0?rdColumn.precision():15) + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATE";
            }else if(info.getColumnType() == ColumnType.DATE){
                type = "DATE";
            }else{
                throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
            }
        }else if(Long.class.getName().equals(infoType)){
            type = "NUMBER(" + rdColumn.precision() + ")";
        }else if(Double.class.getName().equals(infoType)){
            type = "NUMBER(" + rdColumn.precision() + "," + rdColumn.scale() + ")";
        }else if(Float.class.getName().equals(infoType)){
            type = "FLOAT";
//            if(info.getColumnType() == ColumnType.REAL){
//                type = "REAL";
//            }else {
//                type = "FLOAT";
//            }
        }else if(BigDecimal.class.getName().equals(infoType)){
            type = "NUMBER(" + rdColumn.precision() + "," + rdColumn.scale() + ")";
        }else if(byte[].class.getName().equals(infoType)){
            type = "BLOB";
        }else{
            throw new ORMException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }


    @Override
    public String getColumnType(TableColumn column) {
        String type = "";
        if("CLOB".equalsIgnoreCase(column.getType())
                || "BLOB".equalsIgnoreCase(column.getType())
                || "TIMESTAMP".equalsIgnoreCase(column.getType())
                || "DATE".equalsIgnoreCase(column.getType())
                || "FLOAT".equalsIgnoreCase(column.getType())
                || "REAL".equalsIgnoreCase(column.getType())
                ){
            return column.getType().toUpperCase(Locale.ROOT);
        } else if(column.getType().toUpperCase(Locale.ROOT).startsWith("TIMESTAMP(")){
            return column.getType().toUpperCase(Locale.ROOT);
        } else if("CHAR".equalsIgnoreCase(column.getType()) || "VARCHAR2".equalsIgnoreCase(column.getType())){
            type =  column.getType().toUpperCase(Locale.ROOT) + "(" + column.getLength() + ")";
        } else if ("NUMBER".equalsIgnoreCase(column.getType())){
            type = "NUMBER(" + column.getPrecision() + "," + column.getScale() + ")";
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
                type = "CLOB";
            }else if(column.getColumnType() == ColumnType.BLOB){
                type = "BLOB";
            }else if(column.getColumnType() == ColumnType.CLOB) {
                type = "CLOB";
            }else if(column.getColumnType() == ColumnType.BINARY){
                type = "BLOB";
            }else if(column.getColumnType() == ColumnType.CHAR){
                type = "CHAR(" + column.getLength() + ")";
            }else{
                type = "VARCHAR2(" + column.getLength() + ")";
            }
        }else if (Integer.class.getName().equals(className)) {
            type = "NUMBER(" + column.getPrecision() + ")";
        }else if(java.util.Date.class.getName().equals(className)){
            if(column.getColumnType() == ColumnType.LONG){
                type = "NUMBER(" + column.getPrecision() + ", 0)";
            }else if(column.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(column.getColumnType() == ColumnType.DATETIME){
                type = "DATE";
            }else{
                throw new ORMException("Not support type : " + className + "," + column.getColumnType());
            }
        }else if(Long.class.getName().equals(className)){
            type = "NUMBER(" + column.getPrecision() + ")";
        }else if(Double.class.getName().equals(className)){
            type = "NUMBER(" + column.getPrecision() + "," + column.getScale() + ")";
        }else if(Float.class.getName().equals(className)){
            type = "FLOAT";
//            if(info.getColumnType() == ColumnType.REAL){
//                type = "REAL";
//            }else {
//                type = "FLOAT";
//            }
        }else if(BigDecimal.class.getName().equals(className)){
            type = "NUMBER(" + column.getPrecision() + "," + column.getScale() + ")";
        }else if(byte[].class.getName().equals(className)){
            type = "BLOB";
        }else{
            throw new ORMException("Not support type : " + className + "," + column.getColumnType());
        }
        return type;
    }

    @Override
    public String getClassName(TableColumn column) {
        if("CLOB".equalsIgnoreCase(column.getType())
                ||"BLOB".equalsIgnoreCase(column.getType())
                ||"CHAR".equalsIgnoreCase(column.getType())
                ||"VARCHAR2".equalsIgnoreCase(column.getType())){
            return String.class.getName();
        }else if("FLOAT".equalsIgnoreCase(column.getType()) || "REAL".equalsIgnoreCase(column.getType())){
            return Float.class.getName();
        }else if("TIMESTAMP".equalsIgnoreCase(column.getType())
                ||"DATE".equalsIgnoreCase(column.getType())){
            return Date.class.getName();
        }else if("NUMBER".equalsIgnoreCase(column.getType())){
            if(column.getScale() != null && column.getScale().intValue() == 0){
//                if(column.getPrecision() != null && column.getPrecision().intValue() == 15){
//                    return Date.class.getName();
//                }else
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

    public SQLPair parseExpression(Class clazz, Map<String, Object> clazzes, Expression expression) {

        SQLPair sqlPair = null;
        if (expression == null) {
            return sqlPair;
        }
        if (expression.getLeft() != null) {
            String conditionName = parseColumn(expression.getLeft());
            switch (expression.getType()) {
                case CDT_IS_NULL:
                case CDT_IS_EMPTY:
                    sqlPair = new SQLPair(" " + conditionName + " IS NULL ");
                    break;
                case CDT_IS_NOT_NULL:
                case CDT_IS_NOT_EMPTY:
                    sqlPair = new SQLPair(" " + conditionName + " IS NOT NULL ");
                    break;
                default:
                    break;
            }
        }

        if(sqlPair == null){
            return super.parseExpression(clazz, clazzes, expression);
        }
        return sqlPair;

    }

}
