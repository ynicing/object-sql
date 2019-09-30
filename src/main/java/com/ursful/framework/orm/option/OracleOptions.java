package com.ursful.framework.orm.option;

import com.ursful.framework.orm.IQuery;
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
import java.util.ArrayList;
import java.util.List;

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
}
