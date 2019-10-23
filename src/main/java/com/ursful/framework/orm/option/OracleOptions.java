package com.ursful.framework.orm.option;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

public class OracleOptions extends AbstractOptions{

    @Override
    public String keyword() {
        return "oracle";
    }

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException {
        boolean hasSet = false;
        switch (type) {
            case STRING:
                if(columnType == ColumnType.BLOB){
                    if(obj != null) {
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
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
                BigDecimal bg=new BigDecimal((Double)obj);
                bg.setScale(5,BigDecimal.ROUND_HALF_DOWN);
                ps.setDouble(i + 1, bg.doubleValue());
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
        final Integer DURATION_SESSION = new Integer(lobClass.getField(
                "DURATION_SESSION").getInt(null));
        final Integer MODE_READWRITE = new Integer(lobClass.getField(
                "MODE_READWRITE").getInt(null));
        Method createTemporary = lobClass.getMethod("createTemporary",
                new Class[] { Connection.class, boolean.class, int.class });
        Object lob = createTemporary.invoke(null, new Object[] { conn, false,
                DURATION_SESSION });
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

    @Override
    public QueryInfo doQuery(IQuery query, Pageable page) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();

        String alias = "ora_";

        sb.append("SELECT ");
        sb.append(selectColumns(query, alias));
        String order = orders(query, alias);
        boolean hasOrder = !StringUtils.isEmpty(order);
        if(page != null && !hasOrder){
            sb.append(",ROWNUM rn_ ");
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, alias));
        sb.append(joins(query, values));
        int now = sb.length();
        sb.append(wheres(query, values, null));
        int then = sb.length();
        boolean hasCondition = then > now;
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        sb.append(order);
        if(page != null){
            if(hasOrder){
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
        info.setColumns(query.getReturnColumns());

        return info;
    }

    @Override
    public SQLHelper doQuery(Class<?> clazz, String[] names, Terms terms, MultiOrder multiOrder, Integer start, Integer size) {
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
        if(StringUtils.isEmpty(nameStr)){
            sql.append(alias + "." + Expression.EXPRESSION_ALL);
        }else{
            sql.append(nameStr);
        }
        boolean hasOrder = false;
        String order = "";
        if(multiOrder != null) {
            QueryUtils.setMultiOrderAlias(multiOrder, alias);
            String orders = QueryUtils.getOrders(this, multiOrder.getOrders());
            if (!StringUtils.isEmpty(orders)) {
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

        if(terms != null) {
            Condition condition = terms.getCondition();
            QueryUtils.setConditionAlias(condition, alias);
            String conditions = QueryUtils.getConditions(this, clazz, ORMUtils.newList(condition), values);
            if (!StringUtils.isEmpty(conditions)) {
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
    public Table table(Connection connection, String tableName) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Table table = null;
        try {
            String sql = "SELECT * FROM USER_TABLES WHERE  (TABLE_NAME = ? OR TABLE_NAME = ?) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            if(rs.next()) {
                table = new Table();
                table.setName(rs.getString("TABLE_NAME"));
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
        if(table != null){
            try {
                String sql = "SELECT * FROM USER_TAB_COMMENTS WHERE  (TABLE_NAME = ? OR TABLE_NAME = ?) ";
                ps = connection.prepareStatement(sql);
                ps.setString(1, tableName.toUpperCase(Locale.ROOT));
                ps.setString(2, tableName.toLowerCase(Locale.ROOT));
                rs = ps.executeQuery();
                if(rs.next()) {
                    table.setComment(rs.getString("COMMENTS"));
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
        }
        return table;
    }

    @Override
    public List<TableColumn> columns(Connection connection, String tableName) {
        List<TableColumn> columns = new ArrayList<TableColumn>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        Map<String, String> columnComment = new HashMap<String, String>();
        try {
            String sql = "SELECT * FROM USER_COL_COMMENTS WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
            while (rs.next()){
                columnComment.put(rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT), rs.getString("COMMENTS"));
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
            String sql = " SELECT * FROM USER_TAB_COLUMNS WHERE (TABLE_NAME = ? OR TABLE_NAME = ?) ";
            ps = connection.prepareStatement(sql);
            ps.setString(1, tableName.toUpperCase(Locale.ROOT));
            ps.setString(2, tableName.toLowerCase(Locale.ROOT));
            rs = ps.executeQuery();
//            ResultSetMetaData data = rs.getMetaData();

            while (rs.next()){

//                Map<String, String> map = new HashMap<String, String>();
//                for(int i = 1; i <= data.getColumnCount(); i++){
//                    String name = data.getColumnLabel(i);
//                    map.put(name , rs.getString(name));
//                }
                TableColumn tableColumn = new TableColumn(rs.getString("TABLE_NAME").toUpperCase(Locale.ROOT), rs.getString("COLUMN_NAME").toUpperCase(Locale.ROOT));
                tableColumn.setType(rs.getString("DATA_TYPE"));
                tableColumn.setLength(rs.getInt("DATA_LENGTH"));
                tableColumn.setPrecision(rs.getInt("DATA_PRECISION"));
                tableColumn.setScale(rs.getInt("DATA_SCALE"));
                tableColumn.setNullable("Y".equalsIgnoreCase(rs.getString("NULLABLE")));
                tableColumn.setOrder(rs.getInt("COLUMN_ID"));
                tableColumn.setDefaultValue(rs.getString("DATA_DEFAULT"));
                tableColumn.setComment(columnComment.get(tableColumn.getColumn()));
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
                            if("VARCHAR2".equalsIgnoreCase(tableColumn.getType()) || "CHAR".equalsIgnoreCase(tableColumn.getType())){
                                if(tableColumn.getLength() == null){
                                    continue;
                                }
                                if(tableColumn.getLength().intValue() != rdColumn.length()){
                                    needUpdate = true;
                                }
                            }else if("NUMBER".equalsIgnoreCase(tableColumn.getType())){
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
                            if(!needUpdate && !StringUtils.isEmpty(tableColumn.getDefaultValue())){
                                String defaultValue = tableColumn.getDefaultValue().trim();
                                if(defaultValue.startsWith("'") && defaultValue.startsWith("'")){
                                    defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
                                }
                                if(!defaultValue.equals(rdColumn.defaultValue())){
                                    needUpdate = true;
                                }
                            }
                            if(needUpdate) {
                                String temp = columnString(info, rdColumn, false);
                                sqls.add(String.format("ALTER TABLE %s MODIFY %s", tableName, temp));
                                if (!StringUtils.isEmpty(comment) && !comment.equals(tableColumn.getComment())) {
                                    sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, rdColumn.name().toUpperCase(Locale.ROOT), comment));
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
                                sqls.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, rdColumn.name().toUpperCase(Locale.ROOT), comment));
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
                        comments.add(String.format("COMMENT ON COLUMN %s.%s IS '%s'", tableName, rdColumn.name().toUpperCase(Locale.ROOT), comment));
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
                type = "TEXT";
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
                type = "NUMBER(" + rdColumn.datePrecision() + ", 0)";
            }else if(info.getColumnType() == ColumnType.TIMESTAMP){
                type = "TIMESTAMP";
            }else if(info.getColumnType() == ColumnType.DATETIME){
                type = "DATE";
            }else{
                throw new RuntimeException("Not support type : " + infoType + "," + info.getColumnType().name());
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
            throw new RuntimeException("Not support type : " + infoType + "," + info.getColumnType().name());
        }
        return type;
    }
}
