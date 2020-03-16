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

import com.ursful.framework.orm.IMultiQuery;
import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdForeignKey;
import com.ursful.framework.orm.exception.TableAnnotationNotFoundException;
import com.ursful.framework.orm.exception.TableNameNotFoundException;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.annotation.RdUniqueKey;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public abstract class AbstractOptions implements Options{

    public abstract boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Pair pair) throws SQLException;

    public boolean tableExists(Connection connection, RdTable rdTable)  throws TableAnnotationNotFoundException, TableNameNotFoundException {
        if(rdTable == null){
            throw new TableAnnotationNotFoundException();
        }
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        return tableExists(connection, tableName);
    }

    protected String getCoding(Pair pair){
        String defaultCoding = "UTF-8";
        if(pair == null){
            return defaultCoding;
        }
        Map<String, Object> metadata = pair.getMetadata();
        if(metadata != null && metadata.containsKey("coding")){
            defaultCoding = (String) metadata.get("coding");
        }
        return defaultCoding;
    }

    public void setParameter(PreparedStatement ps, Connection connection, String databaseType, int i,Pair pair) throws SQLException {

        Object obj = pair.getValue();
        ColumnType columnType = pair.getColumnType();
        DataType type =  DataType.getDataType(pair.getType());

        boolean hasSet = preSetParameter(ps, connection, databaseType, i, pair);
        if(hasSet){
            return;
        }
        switch (type) {
            case BINARY:
                if(obj != null) {
                    if(obj instanceof byte[]) {
                        ps.setBinaryStream(i + 1, new ByteArrayInputStream((byte[]) obj));
                    }else{
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes(getCoding(pair))));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                }else{
                    ps.setBinaryStream(i + 1, null);
                }
                break;
            case STRING:
                if(columnType == ColumnType.BINARY){
                    if(obj != null) {
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes(getCoding(pair))));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }else{
                        ps.setBinaryStream(i + 1, null);
                    }
                }else if(columnType == ColumnType.BLOB){
                    if(obj != null) {
                        try {
                            ps.setBlob(i + 1, new ByteArrayInputStream(obj.toString().getBytes(getCoding(pair))));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }else{
                        ps.setBinaryStream(i + 1, null);
                    }
                }else if(columnType == ColumnType.CLOB){
                    Clob clob = null;
                    if(obj != null) {
                        clob = new SerialClob(obj.toString().toCharArray());
                    }
                    ps.setClob(i + 1, clob);
                }else {
                    if (obj != null && "".equals(obj.toString().trim())) {
                        obj = null;
                    }
                    ps.setString(i + 1, (String) obj);
                }
                break;
            case DATE:
                if(obj == null){
                    ps.setObject(i + 1, null);
                }else {
                    if(columnType == ColumnType.LONG) {
                        ps.setLong(i + 1, ((Date) obj).getTime());
                    }else if(columnType == ColumnType.DATETIME) {
                        Date date = (Date)obj;
                        ps.setTimestamp(i+1, new Timestamp(date.getTime()));
                    }else{//timestamp
                        ps.setTimestamp(i + 1, new Timestamp(((Date) obj).getTime()));
                    }
                }
                break;
            case DECIMAL:
                BigDecimal decimal = (BigDecimal) obj;
                Map<String, Object> metadata = pair.getMetadata();
                int runningMode = BigDecimal.ROUND_HALF_DOWN;//默认采用四舍五入
                int scale = decimal.scale();
                if(metadata != null){
                    Integer mode = (Integer)metadata.get("runningMode");
                    if(mode != null && mode.intValue() > -1){
                        runningMode = mode.intValue();
                    }
                    Integer scaleValue = (Integer) metadata.get("scale");
                    if(scaleValue != null){//当scale为0时，才会使用 RdColumn中的scale, 不能使用上述 decimal中的scale, 有可能失去精度变成很大
                        scale = scaleValue.intValue();
                    }
                }
                BigDecimal setScale = decimal.setScale(scale, runningMode);
                ps.setBigDecimal(i + 1, setScale);
                break;
            case DOUBLE:
                ps.setObject(i + 1, obj);
                break;
            default:
                ps.setObject(i + 1, obj);
                break;
        }
    }


    @Override
    public QueryInfo doQueryCount(IQuery query) {
        QueryInfo info = new QueryInfo();
        List<Pair> values = new ArrayList<Pair>();
        StringBuffer sb = new StringBuffer();
        sb.append("SELECT ");
        if(query.isDistinct()){
            sb.append(selectColumns(query, null));
        }else {
            sb.append(selectCount());
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        sb.append(joins(query, values));
        sb.append(wheres(query, values, null));
        sb.append(groups(query, null));
        sb.append(havings(query, values, null));
        info.setClazz(Integer.class);
        if(query.isDistinct()) {
            info.setSql("SELECT COUNT(*) FROM (" + sb.toString() + ")  _distinct_table");
        }else{
            info.setSql(sb.toString());
        }
        info.setValues(values);
        return info;
    }


    public String selectCount(){
        return " COUNT(*) ";
    }

    public String selectColumns(IQuery query, String alias){
        StringBuffer sb = new StringBuffer();
        if(query.isDistinct()) {
            sb.append(" DISTINCT ");
        }
        List<Column> returnColumns = query.getReturnColumns();
        List<String> temp = new ArrayList<String>();
        List<String> allAlias = new ArrayList<String>();
        List<String> inColumn = new ArrayList<String>();
        boolean noAlias = false;
        if(returnColumns.isEmpty()){
            String all = null;
            if(StringUtils.isEmpty(alias)){
                noAlias = true;
                all = Expression.EXPRESSION_ALL;
            }else{
                allAlias.add(alias);
                all = alias + "." + Expression.EXPRESSION_ALL;
            }
            temp.add(all);
        }else{
            for(Column column : returnColumns){
                if(column.getAlias() == null && !StringUtils.isEmpty(alias)){
                    column.setAlias(alias);
                }
                inColumn.add(column.getAlias() + "." + column.getName());
                temp.add(parseColumn(column));
                if(Expression.EXPRESSION_ALL.equals(column.getName())){
                    if(!StringUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())){
                        allAlias.add(column.getAlias());
                    }else{
                        noAlias = true;
                    }
                }
            }
        }
        if(!noAlias) {
            List<Order> orders = query.getOrders();
            for (Order order : orders) {
                Column column = order.getColumn();
                QueryUtils.setColumnAlias(column, alias);
                if (!StringUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())
                        && !inColumn.contains(column.getAlias() + "." + column.getName())) {
                    String orderStr = parseColumn(column);
                    temp.add(orderStr);
                }
            }
        }
        sb.append(ORMUtils.join(temp, ","));
        if(sb.length() == 0){
            if(StringUtils.isEmpty(alias)) {
                sb.append(Expression.EXPRESSION_ALL);
            }else{
                sb.append(alias + "." + Expression.EXPRESSION_ALL);
            }
        }
        return sb.toString();
    }

    public String orders(IQuery query, String alias){
        String result = "";
        List<Order> orders = query.getOrders();
        QueryUtils.setOrdersAlias(orders, alias);
        String orderString = getOrders(orders);
        if (orderString != null && !"".equals(orderString)) {
            result = " ORDER BY " + orderString;
        }
        return result;
    }

    public String tables(IQuery query, List<Pair> values, String tableAlias){
        if(query.getTable() != null){
            String tableName = ORMUtils.getTableName(query.getTable());
            if(tableAlias  == null) {
                return tableName;
            }else{
                return tableName + " " + tableAlias;
            }
        }else{
            List<String> words = new ArrayList<String>();
            Map<String, Class<?>> aliasMap = query.getAliasTable();
            Map<String, IQuery> aliasQuery = query.getAliasQuery();
            List<String> aliasList = query.getAliasList();
            for(String alias : aliasList) {
                if(aliasMap.containsKey(alias)) {
                    String tn = ORMUtils.getTableName(aliasMap.get(alias));
                    words.add(tn + " " + alias);
                }else if(aliasQuery.containsKey(alias)){
                    IQuery q = aliasQuery.get(alias);
                    QueryInfo queryInfo = doQuery(q, null);
                    words.add("(" + queryInfo.getSql() + ") " + alias);
                    values.addAll(queryInfo.getValues());
                }
            }
            return ORMUtils.join(words, ",");
        }
    }

    public String wheres(IQuery query, List<Pair> values, String tableAlias){
        String result = "";
        List<Condition> conditions = query.getConditions();
        QueryUtils.setConditionsAlias(conditions, tableAlias);
        String whereCondition = getConditions(query, conditions, values);
        if(whereCondition != null && !"".equals(whereCondition)){
            result =" WHERE " + whereCondition;
        }
        return result;
    }

    public String groups(IQuery query, String alias){
        String result = "";
        List<Column> columns = query.getGroups();
        QueryUtils.setColumnsAlias(columns, alias);
        String groupString = getGroups(columns);
        if(groupString != null && !"".equals(groupString)){
            result =" GROUP BY " + groupString;
        }
        return result;
    }

    public String havings(IQuery query, List<Pair> values, String alias){
        String result = "";
        List<Condition> conditions = query.getHavings();
        QueryUtils.setConditionsAlias(conditions, alias);
        String havingString = getConditions(query, conditions, values);
        if(havingString != null && !"".equals(havingString)){
            result = " HAVING " + havingString;
        }
        return result;
    }

    public String joins(IQuery obj, List<Pair> values){
        List<Join> joins = obj.getJoins();
        StringBuffer sb = new StringBuffer();
        if(joins == null){
            return  sb.toString();
        }
        for(int i = 0; i < joins.size(); i++){
            Join join = joins.get(i);
            String tableName = null;
            Object table = join.getTable();
            if(table instanceof Class) {
                tableName = ORMUtils.getTableName((Class)table);
            }else if(table instanceof IQuery){
                QueryInfo info = doQuery((IQuery)table, null);
                tableName = "(" + info.getSql() + ") ";
                values.addAll(info.getValues());
            }
            switch (join.getType()){
                case FULL_JOIN:
                    sb.append(" FULL JOIN ");
                    break;
                case INNER_JOIN:
                    sb.append(" INNER JOIN ");
                    break;
                case LEFT_JOIN:
                    sb.append(" LEFT JOIN ");
                    break;
                case RIGHT_JOIN:
                    sb.append(" RIGHT JOIN ");
                    break;
            }
            String alias = join.getAlias();

            sb.append(tableName + " " + alias);

            List<Condition> temp = join.getConditions();

            String cdt = getConditions(obj, temp, values);
            if(cdt != null && !"".equals(cdt)) {
                sb.append(" ON ");
                sb.append(cdt);
            }
        }
        return sb.toString();
    }

    protected String getForeignSQL(RdTable table, RdColumn rdColumn, RdForeignKey foreign) {
        String foreignKey = null;
        String foreignTable= null;
        String foreignColumn = null;
        if(foreign == null){
            if(StringUtils.isEmpty(rdColumn.foreignKey())){
                return null;
            }
            foreignKey = rdColumn.foreignKey();
            foreignTable= rdColumn.foreignTable();
            foreignColumn = rdColumn.foreignColumn();
        }else{
            foreignKey = foreign.name();
            foreignTable= foreign.table();
            foreignColumn = foreign.column();
        }
        Assert.isTrue(!StringUtils.isEmpty(foreignKey), "Foreign Key Name Should Not Be Empty.");
        Assert.isTrue(!StringUtils.isEmpty(foreignTable), "Foreign Table Should Not Be Empty.");
        Assert.isTrue(!StringUtils.isEmpty(foreignColumn), "Foreign Column Should Not Be Empty.");
//        constraint JOB_INST_EXEC_FK foreign key (JOB_INSTANCE_ID) references BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
        return String.format("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)", foreignKey, rdColumn.name(), foreignTable, foreignColumn);
    }


    protected String getUniqueSQL(RdTable table, RdColumn rdColumn, RdUniqueKey uniqueKey) {

        if(!rdColumn.unique() && (uniqueKey == null)){
            return null;
        }

        String uniqueName = null;
        String [] uniqueKeys = null;
        if(uniqueKey != null){
            uniqueName = uniqueKey.name();
            uniqueKeys = uniqueKey.columns();
        }else{
            uniqueName = rdColumn.uniqueName();
            uniqueKeys = rdColumn.uniqueKeys();
        }

        if(StringUtils.isEmpty(uniqueName)){
            String [] tabs = table.name().split("_");
            StringBuffer names = new StringBuffer();
            for(String tab : tabs){
                if(tab.length() > 0) {
                    names.append(tab.charAt(0));
                }
            }
            if(uniqueKeys != null && uniqueKeys.length > 0){
                for(String key : uniqueKeys){
                    if(key.length() > 0) {
                        names.append("_" + key.charAt(0));
                    }
                }
            }else{
                names.append("_" + rdColumn.name().charAt(0));
            }
            uniqueName = names.toString().toUpperCase(Locale.ROOT);
        }
        String uniqueKeysStr = ORMUtils.join(uniqueKeys, ",");
        if(StringUtils.isEmpty(uniqueKeysStr)){
            uniqueKeysStr =  rdColumn.name();
        }
        return String.format("CONSTRAINT %s UNIQUE(%s)", uniqueName, uniqueKeysStr);
    }

    protected String columnComment(RdColumn rdColumn){
        String comment = null;
        if(!StringUtils.isEmpty(rdColumn.title())){
            comment =  rdColumn.title();
            if(!StringUtils.isEmpty(rdColumn.description())){
                comment += ";" + rdColumn.description();
            }
        }
        return comment;
    }


    abstract String getColumnType(ColumnInfo info, RdColumn column);

    public SQLPair parseExpression(Class clazz, Map<String,Class<?>> clazzes, Expression expression){

        SQLPair sqlPair = null;
        if(expression == null){
            return sqlPair;
        }
        if(expression.getLeft() == null){
            if(expression.getValue() instanceof IMultiQuery){
                IMultiQuery mq = (IMultiQuery)expression.getValue();
                QueryInfo qinfo = mq.doQuery();
                if(expression.getType() == ExpressionType.CDT_EXISTS) {
                    sqlPair = new SQLPair("EXISTS(" + qinfo.getSql() + ")", qinfo.getValues());
                }else  if(expression.getType() == ExpressionType.CDT_NOT_EXISTS) {
                    sqlPair = new SQLPair("NOT EXISTS(" + qinfo.getSql() + ")", qinfo.getValues());
                }
            }
            return sqlPair;
        }

        String conditionName = parseColumn(expression.getLeft());
        switch (expression.getType()) {
            case CDT_IS_NULL:
                sqlPair = new SQLPair(" "+ conditionName + " IS NULL ");
                break;
            case CDT_IS_NOT_NULL:
                sqlPair = new SQLPair(" "+ conditionName + " IS NOT NULL ");
                break;
            case CDT_IS_EMPTY:
                sqlPair = new SQLPair(" "+ conditionName + " = '' ");
                break;
            case CDT_IS_NOT_EMPTY:
                sqlPair = new SQLPair(" "+ conditionName + " != '' ");
                break;
            default:
                break;
        }
        if(sqlPair != null){
            return sqlPair;
        }

        Object conditionValue = expression.getValue();

        boolean isTrim = ORMUtils.isTrim();
        if(isTrim && (conditionValue instanceof String)){
            conditionValue = ((String) conditionValue).trim();
        }
        if(conditionValue != null ) {
            if (conditionValue instanceof Column) {
                String valueSQL = parseColumn((Column) conditionValue);
                if(expression.getType() == null){
                    sqlPair = new SQLPair(conditionName + " = " + valueSQL);
                }else{
                    switch (expression.getType()) {
                        case CDT_Equal:
                            sqlPair = new SQLPair(" " + conditionName + " = " + valueSQL);
                            break;
                        case CDT_NotEqual:
                            sqlPair = new SQLPair(" " + conditionName + " != " + valueSQL);
                            break;
                        case CDT_More:
                            sqlPair = new SQLPair(" " + conditionName + " > " + valueSQL);
                            break;
                        case CDT_MoreEqual:
                            sqlPair = new SQLPair(" " + conditionName + " >= " + valueSQL);
                            break;
                        case CDT_Less:
                            sqlPair = new SQLPair(" " + conditionName + " < " + valueSQL);
                            break;
                        case CDT_LessEqual:
                            sqlPair = new SQLPair(" " + conditionName + " <= " + valueSQL);
                            break;
                    }
                }
                if(sqlPair != null){
                    return sqlPair;
                }
            }
        }
        if(!ORMUtils.isEmpty(conditionValue)){
            Column column = expression.getLeft();
            ColumnType columnType = null;
            if(ORMUtils.isEmpty(column.getAlias())){
                Map<String, ColumnType> pairMap = ORMUtils.getColumnType(clazz);
                columnType = pairMap.get(column.getName());
            }else{
                if(clazzes != null && clazzes.containsKey(column.getAlias())){
                    Map<String, ColumnType> pairMap = ORMUtils.getColumnType(clazzes.get(column.getAlias()));
                    columnType = pairMap.get(column.getName());
                }
            }
            switch (expression.getType()) {
                case CDT_Equal:
                    sqlPair = new SQLPair(" " + conditionName + " = ?", new Pair(conditionValue,columnType));
                    break;
                case CDT_NotEqual:
                    sqlPair = new SQLPair(" " + conditionName + " != ?", new Pair(conditionValue,columnType));
                    break;
                case CDT_More:
                    sqlPair = new SQLPair(" "+ conditionName + " > ?", new Pair(conditionValue,columnType));
                    break;
                case CDT_MoreEqual:
                    sqlPair = new SQLPair(" "+ conditionName + " >= ?", new Pair(conditionValue,columnType));
                    break;
                case CDT_Less:
                    sqlPair = new SQLPair(" "+ conditionName + " < ?", new Pair(conditionValue,columnType));
                    break;
                case CDT_LessEqual:
                    sqlPair = new SQLPair(" "+ conditionName + " <= ?", new Pair(conditionValue,columnType));
                    break;
                case CDT_Like:
                    sqlPair = new SQLPair(" "+ conditionName + " LIKE ?", new Pair("%" +conditionValue + "%"));
                    break;
                case CDT_NotLike:
                    sqlPair = new SQLPair(" "+ conditionName + " NOT LIKE ?", new Pair("%" +conditionValue + "%"));
                    break;
                case CDT_EndWith:
                    sqlPair = new SQLPair(" "+ conditionName + " LIKE ?", new Pair("%" +conditionValue));
                    break;
                case CDT_StartWith:
                    sqlPair = new SQLPair(" "+ conditionName + " LIKE ?", new Pair(conditionValue + "%"));
                    break;
                case CDT_NotEndWith:
                    sqlPair = new SQLPair(" "+ conditionName + " NOT LIKE ?", new Pair("%" +conditionValue));
                    break;
                case CDT_NotStartWith:
                    sqlPair = new SQLPair(" "+ conditionName + " NOT LIKE ?", new Pair(conditionValue + "%"));
                    break;
                case CDT_In:
                case CDT_NotIn:
                    if(Collection.class.isAssignableFrom(conditionValue.getClass())){
                        List<String> names = new ArrayList<String>();
                        List<Pair> values = new ArrayList<Pair>();
                        Iterator iterator = ((Collection) conditionValue).iterator();
                        while (iterator.hasNext()){
                            Object obj = iterator.next();
                            if(obj != null){
                                if(isTrim && (obj instanceof String)){
                                    String obj2 = ((String) obj).trim();
                                    if(!"".equals(obj2)) {
                                        names.add("?");
                                        values.add(new Pair(obj, columnType));
                                    }
                                }else{
                                    names.add("?");
                                    values.add(new Pair(obj, columnType));
                                }
                            }
                        }
                        if(!names.isEmpty()) {
                            if (expression.getType() == ExpressionType.CDT_In) {
                                sqlPair = new SQLPair(" " + conditionName + " IN (" + ORMUtils.join(names, ",") + ")", values);
                            } else {
                                sqlPair = new SQLPair(" " + conditionName + " NOT IN (" + ORMUtils.join(names, ",") + ")", values);
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        return sqlPair;
    }

    public String parseColumn(Column column){

        if(column == null){
            throw new RuntimeException("QUERY_SQL_COLUMN_IS_NULL this column is null");
        }
        if(column.getName() == null){
            throw new RuntimeException("QUERY_SQL_NAME_IS_NULL this column name is null.");
        }

        StringBuffer sb = new StringBuffer();

        if(!ORMUtils.isEmpty(column.getFunction())){
            if(!ORMUtils.isEmpty(column.getType())){
                String result = getColumnWithOperatorAndFunction(column);
                sb.append(result);
            }else{
                sb.append(column.getFunction());
                sb.append("(");
                if(!ORMUtils.isEmpty(column.getAlias())){
                    sb.append(column.getAlias() + ".");
                }
                sb.append(column.getName());
                sb.append(")");
            }
        }else{
            if(!ORMUtils.isEmpty(column.getType())){
                String result = getColumnWithOperator(column);
                sb.append(result);
            }else{
                String aliasName = column.getName();
                if(!ORMUtils.isEmpty(column.getAlias())){
                    aliasName = column.getAlias() + "." + aliasName;
                }
                if(StringUtils.isEmpty(column.getFormat())){
                    sb.append(aliasName);
                }else{
                    if(column.getValue() != null && column.getValue().getClass().isArray()){
                        Object [] objectList = (Object[])column.getValue();
                        List<Object> objects = new ArrayList<Object>();
                        for(Object obj : objectList){
                            if(obj == column) {
                                objects.add(aliasName);
                            }else if(obj instanceof Column){
                                objects.add(parseColumn((Column) obj));
                            }else{
                                objects.add(obj);
                            }
                        }
                        sb.append(String.format(column.getFormat(), objects.toArray(new Object[objects.size()])));
                    }else {
                        sb.append(String.format(column.getFormat(), aliasName));
                    }
                }
            }
        }
        // operator out of function as name is invalid
        if(!ORMUtils.isEmpty(column.getAsName())){
            sb.append(" AS " + column.getAsName());
        }
        return sb.toString();
    }


    protected String getColumnWithOperator(Column column) {
        String name = ORMUtils.isEmpty(column.getAlias())? column.getName():column.getAlias()  + "." + column.getName();
        String value = null;
        if(column.getValue() instanceof Column){
            value = "(" + parseColumn((Column)column.getValue()) + ")";
        }else{
            if(column.getValue() != null) {
                value = column.getValue().toString();
            }
        }
        String result = getColumnWithOperator(column.getType(), name, value);

        if(result == null){
            result = name + column.getType().getOperator() + value;
        }
        return result;
    }

    public String getColumnWithOperatorAndFunction(Column column) {
        boolean inFunction = column.getOperatorInFunction();
        String name = ORMUtils.isEmpty(column.getAlias())? column.getName():column.getAlias()  + "." + column.getName();
        String value = null;
        if(column.getValue() instanceof Column){
            value = "(" + parseColumn((Column)column.getValue()) + ")";
        }else{
            if(column.getValue() != null) {
                value = column.getValue().toString();
            }
        }

        String type = DatabaseTypeHolder.get();
        String function = column.getFunction();
        OperatorType operatorType = column.getType();
        String result = getColumnWithOperatorAndFunction(function, inFunction, operatorType, name, value);
        return result;
    }


    public String getOrders(List<Order> orders){
        List<String> temp = new ArrayList<String>();
        if(orders != null){
            for(Order order : orders){
                temp.add(parseColumn(order.getColumn()) + " " + order.getOrder());
            }
        }
        String result = ORMUtils.join(temp, ",");
        return result;
    }


    public String getGroups(List<Column> columns){
        List<String> temp = new ArrayList<String>();
        for(Column column : columns){
            temp.add(parseColumn(column));
        }
        String result = ORMUtils.join(temp, ",");
        return result;
    }


    public static String parseColumn(Options options, Column column){
        return options.parseColumn(column);
    }

    //同一张表怎么半？ select * from test t1, test t2


    // a and b or (c or d ) or (c and d)
    public String getConditions(Object queryOrClass, List<Condition> cds, List<Pair> values){
        StringBuffer sql = new StringBuffer();
        if(cds != null) {
            SQLPair sqlPair = null;
            for (Condition condition : cds) {
                if(condition == null){
                    continue;
                }
                List<ConditionObject> exts = condition.getConditions();
                for(ConditionObject ext : exts){
                    Object extObject = ext.getObject();
                    switch (ext.getType()){
                        case AND:
                            if(extObject instanceof Expression) {
                                Expression and = (Expression) extObject;
                                sqlPair = parseExpression(queryOrClass, and);
                            }else if(extObject instanceof SQLPair){
                                sqlPair = (SQLPair)extObject;
                            }
                            if(sqlPair != null && !StringUtils.isEmpty(sqlPair.getSql())) {
                                if (sql.length() == 0) {
                                    sql.append(sqlPair.getSql());
                                } else {
                                    sql.append(" AND " + sqlPair.getSql());
                                }
                                if (sqlPair.getPairs() != null) {//column = column
                                    values.addAll(sqlPair.getPairs());
                                }
                            }
                            break;
                        case OR:
                            if(extObject instanceof Expression) {
                                Expression or = (Expression)extObject;
                                sqlPair = parseExpression(queryOrClass, or);
                            }else if(extObject instanceof SQLPair){
                                sqlPair = (SQLPair)extObject;
                            }
                            if(sqlPair != null && !StringUtils.isEmpty(sqlPair.getSql())) {
                                if (sql.length() == 0) {
                                    sql.append(sqlPair.getSql());
                                } else {
                                    sql.append(" OR " + sqlPair.getSql());
                                }
                                if (sqlPair.getPairs() != null) {//column = column
                                    values.addAll(sqlPair.getPairs());
                                }
                            }
                            break;
                        case AND_OR:
                            Expression[] aor = (Expression[])extObject;
                            List<String> aorStr = new ArrayList<String>();
                            for(Expression orOr : aor){
                                sqlPair = parseExpression(queryOrClass, orOr);
                                if(sqlPair != null && !StringUtils.isEmpty(sqlPair.getSql())) {
                                    aorStr.add(sqlPair.getSql());
                                    if (sqlPair.getPairs() != null) {//column = column
                                        values.addAll(sqlPair.getPairs());
                                    }
                                }
                            }
                            if (aorStr.size() > 0) {
                                if (sql.length() == 0) {
                                    sql.append(" (" + ORMUtils.join(aorStr, " OR ") + ") ");
                                } else {
                                    sql.append(" AND (" + ORMUtils.join(aorStr, " OR ") + ") ");
                                }
                            }
                            break;
                        case OR_AND:
                            Expression[] ands = (Expression[])extObject;
                            List<String> andStr = new ArrayList<String>();
                            for(Expression orAnd : ands){
                                sqlPair = parseExpression(queryOrClass, orAnd);
                                if(sqlPair != null && !StringUtils.isEmpty(sqlPair.getSql())) {
                                    andStr.add(sqlPair.getSql());
                                    if (sqlPair.getPairs() != null) {//column = column
                                        values.addAll(sqlPair.getPairs());
                                    }
                                }
                            }
                            if (andStr.size() > 0) {
                                if (sql.length() == 0) {
                                    sql.append(" (" + ORMUtils.join(andStr, " AND ") + ") ");
                                } else {
                                    sql.append(" OR (" + ORMUtils.join(andStr, " AND ") + ") ");
                                }
                            }
                            break;
                        case OR_OR:
                            Expression[] ors = (Expression[])extObject;
                            List<String> orStr = new ArrayList<String>();
                            for(Expression orOr : ors){
                                sqlPair = parseExpression(queryOrClass, orOr);
                                if(sqlPair != null && !StringUtils.isEmpty(sqlPair.getSql())) {
                                    orStr.add(sqlPair.getSql());
                                    if (sqlPair.getPairs() != null) {//column = column
                                        values.addAll(sqlPair.getPairs());
                                    }
                                }
                            }
                            if (orStr.size() > 0) {
                                if (sql.length() == 0) {
                                    sql.append(" (" + ORMUtils.join(orStr, " OR ") + ") ");
                                } else {
                                    sql.append(" OR (" + ORMUtils.join(orStr, " OR ") + ") ");
                                }
                            }
                            break;
                    }
                }
            }
        }
        return sql.toString();
    }

    public String getConditions(Class clazz, Express [] expresses, List<Pair> values){
        List<String> ands = new ArrayList<String>();
        if(expresses != null) {
            for (int i = 0; i < expresses.length; i++) {
                Express express = expresses[i];
                if(express == null){
                    continue;
                }
                Expression expression = express.getExpression();
                SQLPair sqlPair = parseExpression(clazz, expression);
                if (sqlPair != null) {
                    ands.add(sqlPair.getSql());
                    if (sqlPair.getPairs() != null) {
                        values.addAll(sqlPair.getPairs());
                    }
                }
            }
        }
        return ORMUtils.join(ands, " AND ");
    }

    public SQLPair parseExpression(Object clazz, Expression expression){
        if(clazz instanceof Class){
            return parseExpression((Class)clazz, null, expression);
        }else if(clazz instanceof IQuery){
            IQuery query = (IQuery)clazz;
            return parseExpression(query.getTable(), query.getAliasTable(), expression);
        }else{
            return parseExpression(null, null, expression);
        }
    }



    public String getTableName(RdTable table)  throws TableAnnotationNotFoundException, TableNameNotFoundException{
        return null;
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
            return name;//默认
        }
    }

}
