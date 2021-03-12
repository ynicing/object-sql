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
import com.ursful.framework.orm.exception.ORMError;
import com.ursful.framework.orm.exception.ORMException;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.annotation.RdUniqueKey;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.utils.ORMUtils;

import javax.sql.rowset.serial.SerialClob;
import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;

public abstract class AbstractOptions implements Options{

    public QueryInfo doQuery(IQuery query){
        return doQuery(query, null);
    }

    public abstract boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Pair pair) throws SQLException;

    public boolean tableExists(Connection connection, RdTable rdTable)  throws ORMException {
        if(rdTable == null){
            throw new ORMException(ORMError.TABLE_ANNOTATION_IS_NULL);
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
                if(obj == null) {
                    ps.setObject(i + 1, null);
                }else{
                    BigDecimal decimal = (BigDecimal) obj;
                    Map<String, Object> metadata = pair.getMetadata();
                    int runningMode = BigDecimal.ROUND_UNNECESSARY;
                    int scale = decimal.scale();
                    if (metadata != null) {
                        Integer scaleValue = (Integer) metadata.get("scale");
                        int newScale = scale;
                        if (scaleValue != null) {//当scale为0时，才会使用 RdColumn中的scale, 不能使用上述 decimal中的scale, 有可能失去精度变成很大
                            newScale = scaleValue.intValue();
                        }
                        Integer mode = (Integer) metadata.get("runningMode");//优先级最高，比ORMUTils。runningMode高
                        if (mode != null && mode.intValue() > -1) {
                            runningMode = mode.intValue();
                            scale = newScale;
                        }else if(ORMUtils.getRunningMode() > -1){
                            runningMode = ORMUtils.getRunningMode();
                            scale = newScale;
                        }
                    }else{
                        if(ORMUtils.getRunningMode() > -1){
                            runningMode = ORMUtils.getRunningMode();
                        }
                    }
                    BigDecimal setScale = decimal.setScale(scale, runningMode);
                    ps.setBigDecimal(i + 1, setScale);
                }
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
            sb.append(selectColumns(query, null, null));
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

    public String selectColumns(IQuery query, String alias, Map<String, String> asNames){
        if(asNames == null){
            asNames = new HashMap<String, String>();
        }
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
            if(ORMUtils.isEmpty(alias)){
                noAlias = true;
                all = Expression.EXPRESSION_ALL;
            }else{
                allAlias.add(alias);
                all = alias + "." + Expression.EXPRESSION_ALL;
            }
            temp.add(all);
        }else{
            for(Column column : returnColumns){
                if(column.getAlias() == null && !ORMUtils.isEmpty(alias)){
                    column.setAlias(alias);
                }
                inColumn.add(column.getAlias() + "." + column.getName());
                String columnSQL = parseColumn(column);
                temp.add(parseColumn(column));

                String fm = columnSQL.toUpperCase(Locale.ROOT);
                String word = " AS ";
                int index = fm.indexOf(word);
                if(index > -1) {
                    String asName = fm.substring(index + word.length());
                    asNames.put(asName.trim(), fm.substring(0, index));
                }

                if(Expression.EXPRESSION_ALL.equals(column.getName())){
                    if(!ORMUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())){
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
                if(!asNames.containsKey(column.getName().toUpperCase(Locale.ROOT))) {
                    QueryUtils.setColumnAlias(column, alias);
                }
                if (!ORMUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())
                        && !inColumn.contains(column.getAlias() + "." + column.getName())) {
                    String orderStr = parseColumn(column);
                    temp.add(orderStr);
                }
            }
        }
        sb.append(ORMUtils.join(temp, ","));
        if(sb.length() == 0){
            if(ORMUtils.isEmpty(alias)) {
                sb.append(Expression.EXPRESSION_ALL);
            }else{
                sb.append(alias + "." + Expression.EXPRESSION_ALL);
            }
        }
        return sb.toString();
    }

    public String orders(IQuery query, String alias, Map<String, String> asNames){
        if (asNames == null){
            asNames = new HashMap<String, String>();
        }
        String result = "";
        List<Order> orders = query.getOrders();
        for(Order order : orders) {
            Column column = order.getColumn();
            if(ORMUtils.isEmpty(column.getAlias())){
                String key = column.getName().toUpperCase(Locale.ROOT);
                if(!asNames.containsKey(key)){
                    QueryUtils.setColumnAlias(order.getColumn(), alias);
                }else{
                    column.setName(asNames.get(key));
                }
            }
        }
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
            Map<String, Object> aliasMap = query.getAliasTable();
            List<String> aliasList = query.getAliasList();
            for(String alias : aliasList) {
                if(aliasMap.containsKey(alias)) {
                    Object object = aliasMap.get(alias);
                    if(object instanceof Class) {
                        String tn = ORMUtils.getTableName((Class) object);
                        words.add(tn + " " + alias);
                    }else if(object instanceof IQuery){
                        IQuery q = (IQuery)aliasMap.get(alias);
                        QueryInfo queryInfo = doQuery(q, null);
                        words.add("(" + queryInfo.getSql() + ") " + alias);
                        values.addAll(queryInfo.getValues());
                    }else{
                        words.add(object.toString() + " " + alias);
                    }
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
            }else{// String.
                tableName =  join.getTable().toString();
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
            if(ORMUtils.isEmpty(rdColumn.foreignKey())){
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
        ORMUtils.whenEmpty(foreignKey, "Foreign Key Name Should Not Be Empty.");
        ORMUtils.whenEmpty(foreignTable, "Foreign Table Should Not Be Empty.");
        ORMUtils.whenEmpty(foreignColumn, "Foreign Column Should Not Be Empty.");
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

        if(ORMUtils.isEmpty(uniqueName)){
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
        if(ORMUtils.isEmpty(uniqueKeysStr)){
            uniqueKeysStr =  rdColumn.name();
        }
        return String.format("CONSTRAINT %s UNIQUE(%s)", uniqueName, uniqueKeysStr);
    }

    protected String columnComment(RdColumn rdColumn){
        String comment = null;
        if(!ORMUtils.isEmpty(rdColumn.title())){
            comment =  rdColumn.title();
            if(!ORMUtils.isEmpty(rdColumn.description())){
                comment += ";" + rdColumn.description();
            }
        }
        return comment;
    }


    abstract String getColumnType(ColumnInfo info, RdColumn column);

    public SQLPair parseExpression(Class clazz, Map<String,Object> clazzes, Expression expression){

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
            }else if(conditionValue instanceof IQuery){
                QueryInfo newQueryInfo = doQuery((IQuery)conditionValue);
                String sql = null;
                if(ExpressionType.CDT_In == expression.getType()){
                    sql = String.format(" %s IN (%s) ", conditionName, newQueryInfo.getSql());
                }else{
                    sql = String.format(" %s NOT IN (%s) ", conditionName, newQueryInfo.getSql());
                }
                if(sql != null) {
                    sqlPair = new SQLPair(sql, newQueryInfo.getValues());
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
                    Object object = clazzes.get(column.getAlias());
                    if(object instanceof Class) {
                        Map<String, ColumnType> pairMap = ORMUtils.getColumnType((Class)object);
                        columnType = pairMap.get(column.getName());
                    }
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
                if(ORMUtils.isEmpty(column.getFormat())){
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
                    boolean group = false;
                    Object extObject = ext.getObject();
                    switch (ext.getType()){
                        case AND:
                            if(extObject instanceof Expression) {
                                Expression and = (Expression) extObject;
                                sqlPair = parseExpression(queryOrClass, and);
                            }else if(extObject instanceof SQLPair){
                                sqlPair = (SQLPair)extObject;
                            }else if(extObject instanceof Condition){
                                List<Pair> pairList = new ArrayList<Pair>();
                                String sqlCondition = getConditions(queryOrClass, ORMUtils.newList((Condition)extObject), pairList);
                                sqlPair = new SQLPair(sqlCondition, pairList);
                                group = true;
                            }
                            if(sqlPair != null && !ORMUtils.isEmpty(sqlPair.getSql())) {
                                if(group){
                                    if (sql.length() == 0) {
                                        sql.append("(" + sqlPair.getSql() + ")");
                                    } else {
                                        sql.append(" AND (" + sqlPair.getSql() + ")");
                                    }
                                }else {
                                    if (sql.length() == 0) {
                                        sql.append(sqlPair.getSql());
                                    } else {
                                        sql.append(" AND " + sqlPair.getSql());
                                    }
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
                            }else if(extObject instanceof Condition){
                                List<Pair> pairList = new ArrayList<Pair>();
                                String sqlCondition = getConditions(queryOrClass, ORMUtils.newList((Condition)extObject), pairList);
                                sqlPair = new SQLPair(sqlCondition, pairList);
                                group = true;
                            }
                            if(sqlPair != null && !ORMUtils.isEmpty(sqlPair.getSql())) {
                                if(group){
                                    if (sql.length() == 0) {
                                        sql.append("(" + sqlPair.getSql() + ")");
                                    } else {
                                        sql.append(" OR (" + sqlPair.getSql() + ")");
                                    }
                                }else {
                                    if (sql.length() == 0) {
                                        sql.append(sqlPair.getSql());
                                    } else {
                                        sql.append(" OR " + sqlPair.getSql());
                                    }
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
                                if(sqlPair != null && !ORMUtils.isEmpty(sqlPair.getSql())) {
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
                                if(sqlPair != null && !ORMUtils.isEmpty(sqlPair.getSql())) {
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
                                if(sqlPair != null && !ORMUtils.isEmpty(sqlPair.getSql())) {
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

    public abstract String getCaseSensitive(String name, int sensitive);

    public String getTableName(RdTable rdTable) throws ORMException{
        if(rdTable == null){
            throw new ORMException(ORMError.TABLE_ANNOTATION_IS_NULL);
        }
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        if (ORMUtils.isEmpty(tableName)){
            throw new ORMException(ORMError.TABLE_NAME_IS_EMPTY);
        }
        return tableName;
    }
}
