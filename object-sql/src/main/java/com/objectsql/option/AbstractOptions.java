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

import com.objectsql.IBaseQuery;
import com.objectsql.IMultiQuery;
import com.objectsql.annotation.*;
import com.objectsql.exception.ORMException;
import com.objectsql.support.*;
import com.objectsql.IQuery;
import com.objectsql.query.QueryUtils;
import com.objectsql.utils.ORMUtils;
import org.springframework.util.StringUtils;

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
            throw new ORMException("Table annotation(RdTable) is null.");
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
                        if(obj instanceof Long){
                            ps.setLong(i + 1, (Long)obj);
                        }else {
                            ps.setLong(i + 1, ((Date) obj).getTime());
                        }
                    }else if(columnType == ColumnType.DATETIME) {
                        Date date = (Date)obj;
                        ps.setTimestamp(i+1, new Timestamp(date.getTime()));
                    }else if(columnType == ColumnType.DATE) {
                        Date date = (Date)obj;
                        ps.setDate(i+1, new java.sql.Date(date.getTime()));
                    }else if(columnType == ColumnType.TIME) {
                        Date date = (Date)obj;
                        ps.setTime(i+1, new java.sql.Time(date.getTime()));
                    }else if(columnType == ColumnType.YEAR) {
                        Date date = (Date)obj;
                        Calendar calendar = Calendar.getInstance();
                        calendar.setTime(date);
                        ps.setInt(i + 1, calendar.get(Calendar.YEAR));
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
        String groups = groups(query, null);
        if(query.isDistinct()){
            sb.append(selectColumns(query, null, null, values));
        }else {
            if(!ORMUtils.isEmpty(groups)){
                List<Column> groupSelect = query.getGroupCountSelectColumns();
                if(groupSelect == null || groupSelect.isEmpty()){
                    sb.append(selectColumns(query, null, null, values));
                }else{
                    sb.append(groupCountSelect(query));
                }
            }else {
                sb.append(selectCount());
            }
        }
        sb.append(" FROM ");
        sb.append(tables(query, values, null));
        if(query instanceof IMultiQuery) {
            sb.append(joins((IMultiQuery)query, values));
        }
        sb.append(wheres(query, values, null));
        sb.append(groups);
        sb.append(havings(query, values, null));
        info.setClazz(Integer.class);
        if(query.isDistinct()) {
            info.setSql("SELECT COUNT(*) FROM (" + sb.toString() + ")  distinct_table_");
        }else{
            if(ORMUtils.isEmpty(groups)){
                info.setSql(sb.toString());
            }else {
                info.setSql("SELECT COUNT(*) FROM (" + sb.toString() + ")  group_table_");
            }
        }
        info.setValues(values);
        if (TextTransformType.LOWER == query.textTransformType()){
            info.setSql(info.getSql().toLowerCase(Locale.ROOT));
        }else if(TextTransformType.UPPER == query.textTransformType()){
            info.setSql(info.getSql().toUpperCase(Locale.ROOT));
        }
        return info;
    }

    public String selectCount(){
        return " COUNT(*) ";
    }

    public String selectColumns(IQuery query, String alias, Map<String, String> asNames, List<Pair> values){
        if(asNames == null){
            asNames = new HashMap<String, String>();
        }
        StringBuffer sb = new StringBuffer();
        if(query.isDistinct()) {
            sb.append(" DISTINCT ");
        }
        List<Column> returnColumns = query.getFinalReturnColumns();
        List<String> temp = new ArrayList<String>();
        List<String> allAlias = new ArrayList<String>();
        //List<String> inColumn = new ArrayList<String>();
        boolean noAlias = false;
        if(returnColumns.isEmpty()){
            String all = null;
            if(ORMUtils.isEmpty(alias)){
                noAlias = true;
                all = Column.ALL;
            }else{
                allAlias.add(alias);
                all = alias + "." + Column.ALL;
            }
            temp.add(all);
        }else{
            for(Column column : returnColumns){
                if(column.getQuery() != null){
                    QueryInfo queryInfo = doQuery(column.getQuery());
                    String tempSQL = null;
                    if(ORMUtils.isEmpty(column.getAsName())){
                        tempSQL = "(" + queryInfo.getSql() + ")";
                    }else {
                        tempSQL = "(" + queryInfo.getSql() + ") AS " + column.getAsName();
                    }
                    if(temp.contains(tempSQL)){
                        continue;
                    }
                    temp.add(tempSQL);
                    values.addAll(queryInfo.getValues());
                }else{
                    if(column.getAlias() == null && !ORMUtils.isEmpty(alias)){
                        column.setAlias(alias);
                    }
                    //inColumn.add(column.getAlias() + "." + column.getName());
                    String tempSQL = parseColumn(column);
                    if(temp.contains(tempSQL)){
                        continue;
                    }
                    temp.add(tempSQL);
                    //column format id in(a,b,c), 导致编程 id in(A,B,C)
                    String fm = parseColumn(column);//.toUpperCase(Locale.ROOT);
                    String word = " AS ";
                    int index = fm.indexOf(word);
                    if(index > -1) {
                        String asName = fm.substring(index + word.length());
                        asNames.put(asName.trim(), fm.substring(0, index));
                    }

                    if(Column.ALL.equals(column.getName())){
                        if(!ORMUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())){
                            allAlias.add(column.getAlias());
                        }else{
                            noAlias = true;
                        }
                    }
                }

            }
        }
//        if(!noAlias) {
//            List<Order> orders = query.getOrders();
//            for (Order order : orders) {
//                Column column = order.getColumn();
//                if(column instanceof CaseColumn){
//                    //todo 从 CaseColumn获取列
//                    CaseColumn caseColumn = (CaseColumn) column;
//                    StringBuffer caseSql = new StringBuffer();
//                    Map<Condition, Object> conditionMap = caseColumn.getConditions();
//                    Set<Condition> conditions = conditionMap.keySet();
//                    caseSql.append("CASE ");
//                    for(Condition condition :conditions){
//                        Object value = conditionMap.get(condition);
//                        caseSql.append("WHEN " + parseConditions(condition) + " THEN " + (value instanceof Number? value.toString() + " ":"'" + value.toString() +"' "));
//                    }
//
//                    Object eObject = caseColumn.getElseValue();
//                    caseSql.append("ELSE " + (eObject instanceof Number? eObject.toString():"'" + eObject.toString() +"'") + " END");
//
//                    //忽略 as name
//                    temp.add("(" + caseSql.toString() + ") " + order.getOrder());
//                }else {
//                    if (!asNames.containsKey(column.getName().toUpperCase(Locale.ROOT))) {
//                        QueryUtils.setColumnAlias(column, alias);
//                    }
//                    if (!ORMUtils.isEmpty(column.getAlias()) && !allAlias.contains(column.getAlias())
//                            && !inColumn.contains(column.getAlias() + "." + column.getName())) {
//                        String orderStr = parseColumn(column);
//                        temp.add(orderStr);
//                    }
//                }
//            }
//        }
        sb.append(ORMUtils.join(temp, ","));
        if(sb.length() == 0){
            if(ORMUtils.isEmpty(alias)) {
                sb.append(Column.ALL);
            }else{
                sb.append(alias + "." + Column.ALL);
            }
        }
        return sb.toString();
    }

    public String groupCountSelect(IQuery query){

        List<Column> otherColumns = query.getGroupCountSelectColumns();
        List<String> temp = new ArrayList<String>();
        for(Column column : otherColumns){
            String sql = parseColumn(column);
            if(!temp.contains(sql)) {
                temp.add(sql);
            }
        }
        return ORMUtils.join(temp, ",");
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
        //只能获取class，嵌套sql 无法获取
        Map<String, Class> aliasClass = new HashMap<String, Class>();
        if(query instanceof IBaseQuery){
            aliasClass.put(alias, ((IBaseQuery)query).getTable());
        }else if(query instanceof IMultiQuery){
            IMultiQuery multiQuery = (IMultiQuery) query;
            List<Join> joins = multiQuery.getJoins();
            for(Join join : joins){
                String joinAlias = join.getAlias();
                Object aliasObject = join.getTable();
                if(aliasObject instanceof Class) {
                    aliasClass.put(joinAlias, (Class<?>)aliasObject);
                }
            }
            Map<String, Object> table = multiQuery.getAliasTable();
            for(String aliasName : table.keySet()){
                Object aliasObject = table.get(aliasName);
                if(aliasObject instanceof Class) {
                    aliasClass.put(aliasName, (Class<?>)aliasObject);
                }
            }
        }

        String orderString = getOrders(orders, aliasClass);
        if (orderString != null && !"".equals(orderString)) {
            result = " ORDER BY " + orderString;
        }
        return result;
    }

    public String tables(IQuery query, List<Pair> values, String tableAlias){
        if(query instanceof IBaseQuery){//base query
            String tableName = ORMUtils.getTableName(query.getTable());
            if(tableAlias  == null) {
                return tableName;
            }else{
                return tableName + " " + tableAlias;
            }
        }else{
            IMultiQuery multiQuery = (IMultiQuery)query;
            List<String> words = new ArrayList<String>();
            Map<String, Object> aliasMap = multiQuery.getAliasTable();
            List<String> aliasList = multiQuery.getAliasList();
            for(String alias : aliasList) {
                if(aliasMap.containsKey(alias)) {
                    Object object = aliasMap.get(alias);
                    if(object instanceof Class) {
                        String tn = ORMUtils.getTableName((Class) object);
                        words.add(tn + " " + alias);
                    }else if(object instanceof IQuery){
                        IQuery q = (IQuery)aliasMap.get(alias);
                        QueryInfo queryInfo = doQuery(q, q.getPageable());
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

    public String joins(IMultiQuery obj, List<Pair> values){
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
        if(foreign == null){
            return null;
        }
        String foreignKey = foreign.name();
        String foreignTable = ORMUtils.getTableName(foreign.foreignTable());
        String foreignColumn = ORMUtils.join(foreign.foreignColumns(), ",");
        String column = ORMUtils.join(foreign.columns(), ",");
        if (ORMUtils.isEmpty(column)){
            column = rdColumn.name();
        }
        ORMUtils.whenEmpty(foreignKey, "Foreign Key Name Should Not Be Empty.");
        ORMUtils.whenEmpty(foreignTable, "Foreign Table Should Not Be Empty.");
        ORMUtils.whenEmpty(foreignColumn, "Foreign Column Should Not Be Empty.");
//        constraint JOB_INST_EXEC_FK foreign key (JOB_INSTANCE_ID) references BATCH_JOB_INSTANCE(JOB_INSTANCE_ID)
        return String.format("CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)", foreignKey, column, foreignTable, foreignColumn);
    }

    protected String getUniqueSQL(RdTable table, RdColumn rdColumn, RdUniqueKey uniqueKey) {

        if(uniqueKey == null){
            return null;
        }
        String uniqueName = uniqueKey.name();
        String [] uniqueKeys = uniqueKey.columns();
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
                        case CDT_EQUAL:
                            sqlPair = new SQLPair(" " + conditionName + " = " + valueSQL);
                            break;
                        case CDT_NOT_EQUAL:
                            sqlPair = new SQLPair(" " + conditionName + " != " + valueSQL);
                            break;
                        case CDT_MORE:
                            sqlPair = new SQLPair(" " + conditionName + " > " + valueSQL);
                            break;
                        case CDT_MORE_EQUAL:
                            sqlPair = new SQLPair(" " + conditionName + " >= " + valueSQL);
                            break;
                        case CDT_LESS:
                            sqlPair = new SQLPair(" " + conditionName + " < " + valueSQL);
                            break;
                        case CDT_LESS_EQUAL:
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
                if(ExpressionType.CDT_IN == expression.getType()){
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
            ColumnInfo columnInfo = null;
            if(ORMUtils.isEmpty(column.getAlias()) && clazz != null){
                Map<String, ColumnInfo> pairMap = ORMUtils.getColumnColumnInfo(clazz);
                columnInfo = pairMap.get(column.getName());
            }else{
                if(clazzes != null && clazzes.containsKey(column.getAlias())){
                    Object object = clazzes.get(column.getAlias());
                    if(object instanceof Class) {
                        Map<String, ColumnInfo> pairMap = ORMUtils.getColumnColumnInfo((Class) object);
                        columnInfo = pairMap.get(column.getName());
                    }
                }
            }
            switch (expression.getType()) {
                case CDT_EQUAL:
                    sqlPair = new SQLPair(" " + conditionName + " = ?", new Pair(columnInfo, conditionValue));
                    break;
                case CDT_NOT_EQUAL:
                    sqlPair = new SQLPair(" " + conditionName + " != ?", new Pair(columnInfo, conditionValue));
                    break;
                case CDT_MORE:
                    sqlPair = new SQLPair(" "+ conditionName + " > ?", new Pair(columnInfo, conditionValue));
                    break;
                case CDT_MORE_EQUAL:
                    sqlPair = new SQLPair(" "+ conditionName + " >= ?", new Pair(columnInfo, conditionValue));
                    break;
                case CDT_LESS:
                    sqlPair = new SQLPair(" "+ conditionName + " < ?", new Pair(columnInfo, conditionValue));
                    break;
                case CDT_LESS_EQUAL:
                    sqlPair = new SQLPair(" "+ conditionName + " <= ?", new Pair(columnInfo, conditionValue));
                    break;
                case CDT_LIKE:
                    sqlPair = new SQLPair(" "+ conditionName + " LIKE ?", new Pair(columnInfo, "%" +conditionValue + "%"));
                    break;
                case CDT_NOT_LIKE:
                    sqlPair = new SQLPair(" "+ conditionName + " NOT LIKE ?", new Pair(columnInfo, "%" +conditionValue + "%"));
                    break;
                case CDT_END_WITH:
                    sqlPair = new SQLPair(" "+ conditionName + " LIKE ?", new Pair(columnInfo, "%" +conditionValue));
                    break;
                case CDT_START_WITH:
                    sqlPair = new SQLPair(" "+ conditionName + " LIKE ?", new Pair(columnInfo, conditionValue + "%"));
                    break;
                case CDT_NOT_END_WITH:
                    sqlPair = new SQLPair(" "+ conditionName + " NOT LIKE ?", new Pair(columnInfo, "%" +conditionValue));
                    break;
                case CDT_NOT_START_WITH:
                    sqlPair = new SQLPair(" "+ conditionName + " NOT LIKE ?", new Pair(columnInfo, conditionValue + "%"));
                    break;
                case CDT_BETWEEN:
                    List<Pair> vs = new ArrayList<Pair>();
                    StringBuffer btSql = new StringBuffer(" (" + conditionName + " BETWEEN ");
                    if(expression.getValue() instanceof Column){
                        btSql.append(parseColumn((Column) expression.getValue()));
                    }else{
                        btSql.append("?");
                        vs.add(new Pair(columnInfo, conditionValue));
                    }
                    btSql.append(" AND ");
                    if(expression.getAndValue() instanceof Column){
                        btSql.append(parseColumn((Column) expression.getAndValue()));
                    }else{
                        btSql.append("?");
                        Object andConditionValue = expression.getAndValue();
                        if(andConditionValue instanceof String){
                            if(isTrim){
                                andConditionValue = ((String) andConditionValue).trim();
                            }
                        }
                        vs.add(new Pair(columnInfo, andConditionValue));
                    }
                    btSql.append(")");
                    sqlPair = new SQLPair(btSql.toString(), vs);
                    break;
                case CDT_IN:
                case CDT_NOT_IN:
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
                                        values.add(new Pair(columnInfo, obj));
                                    }
                                }else{
                                    names.add("?");
                                    values.add(new Pair(columnInfo, obj));
                                }
                            }
                        }
                        if(!names.isEmpty()) {
                            if (expression.getType() == ExpressionType.CDT_IN) {
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
            throw new ORMException("QUERY_SQL_COLUMN_IS_NULL this column is null");
        }
        StringBuffer sb = new StringBuffer();
        if(column instanceof CaseColumn){
            CaseColumn caseColumn = (CaseColumn) column;
            StringBuffer caseSql = new StringBuffer();
            Map<Condition, Object> conditionMap = caseColumn.getConditions();
            Set<Condition> conditions = conditionMap.keySet();
            caseSql.append("CASE ");
            for(Condition condition :conditions){
                Object value = conditionMap.get(condition);
                caseSql.append("WHEN " + parseConditions(condition) + " THEN " + parseCaseColumnValue(value) + " ");
            }

            Object eObject = caseColumn.getElseValue();
            caseSql.append("ELSE " + parseCaseColumnValue(eObject) + " END");
            if(StringUtils.isEmpty(caseColumn.getFunction())){
                sb.append("(" + caseSql.toString() + ")");
            }else {
                sb.append(caseColumn.getFunction() + "(" + caseSql.toString() + ")");
            }
            if(!StringUtils.isEmpty(column.getAsName())){
                sb.append(" AS " + column.getAsName());
            }
            return sb.toString();
        }
        if(column.getName() == null && ORMUtils.isEmpty(column.getAlias())){
            throw new ORMException("QUERY_SQL_NAME_IS_NULL this column name is null.");
        }

        if(column.getName() == null && !ORMUtils.isEmpty(column.getAsName())){
            sb.append("NULL AS " + column.getAsName());
            return sb.toString();
        }
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
                            }else if(obj instanceof LambdaQuery){
                                objects.add((((LambdaQuery) obj).getColumnName()));
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

    public String getOrders(List<Order> orders, Class<?> clazz){
        Map<String, Class> aliasClass = new HashMap<String, Class>();
        aliasClass.put(null, clazz);
        return getOrders(orders, aliasClass);
    }

    private ColumnInfo getColumnInfo(Class<?> clazz, String column){
        if (clazz == null || ORMUtils.isEmpty(column)){
            return null;
        }
        List<ColumnInfo> columnInfoList = ORMUtils.getColumnInfo(clazz);
        for (ColumnInfo info :columnInfoList){
            if (column.equalsIgnoreCase(info.getColumnName())){
                return info;
            }
        }
        return null;
    }

    private String getSortOptionFormat(ColumnInfo columnInfo){
        if (columnInfo == null){
            return null;
        }
        RdSort sort = columnInfo.getField().getAnnotation(RdSort.class);
        if (sort == null){
            return null;
        }
        RdSortOption [] options = sort.value();
        for (RdSortOption option :options){
            if (keyword().equalsIgnoreCase(option.keyword())){
                return option.format();
            }
        }
        return null;
    }

    public String parseCaseColumnValue(Object object){
        if(object instanceof Number){
            return object.toString();
        }else if(object instanceof Column){
            return parseColumn((Column) object);
        }else{
            if(object == null){
                return "NULL";
            }
            return "'" + object.toString().replace("'", "") + "'";
        }
    }

    public String getOrders(List<Order> orders, Map<String, Class> aliasClass){
        List<String> temp = new ArrayList<String>();
        if(orders != null){
            for(Order order : orders){
                //优先采用order内的column.format
                if(order.getColumn() instanceof CaseColumn){
                    CaseColumn caseColumn = (CaseColumn) order.getColumn();
                    StringBuffer caseSql = new StringBuffer();
                    Map<Condition, Object> conditionMap = caseColumn.getConditions();
                    Set<Condition> conditions = conditionMap.keySet();
                    caseSql.append("CASE ");
                    for(Condition condition :conditions){
                        Object value = conditionMap.get(condition);
                        caseSql.append("WHEN " + parseConditions(condition) + " THEN " + parseCaseColumnValue(value) + " ");
                    }

                    Object eObject = caseColumn.getElseValue();
                    caseSql.append("ELSE " + parseCaseColumnValue(eObject) + " END");
                    //忽略 as name
                    if(StringUtils.isEmpty(caseColumn.getFunction())){
                        temp.add("(" + caseSql.toString() + ") " + order.getOrder());
                    }else {
                        temp.add(caseColumn.getFunction() + "(" + caseSql.toString() + ") " + order.getOrder());
                    }
                }else {
                    if (ORMUtils.isEmpty(order.getColumn().getFormat())) {
                        Class<?> clazz = aliasClass.get(order.getColumn().getAlias());
                        ColumnInfo columnInfo = getColumnInfo(clazz, order.getColumn().getName());
                        String format = getSortOptionFormat(columnInfo);
                        if (ORMUtils.isEmpty(format)) {
                            temp.add(parseColumn(order.getColumn()) + " " + order.getOrder());
                        } else {
                            temp.add(String.format(format, parseColumn(order.getColumn())) + " " + order.getOrder());
                        }
                    } else {
                        temp.add(parseColumn(order.getColumn()) + " " + order.getOrder());
                    }
                }
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


    public String parseConditions(Condition condition){
        StringBuffer sql = new StringBuffer();
        if(condition == null){
            return sql.toString();
        }
        SQLPair sqlPair = null;
        List<ConditionObject> exts = condition.getConditions();
        List<Pair> values = new ArrayList<Pair>();
        for(ConditionObject ext : exts){
            boolean group = false;
            Object extObject = ext.getObject();
            switch (ext.getType()){
                case AND:
                    if(extObject instanceof Expression) {
                        Expression and = (Expression) extObject;
                        sqlPair = parseExpression(null, and);
                    }else if(extObject instanceof SQLPair){
                        sqlPair = (SQLPair)extObject;
                    }else if(extObject instanceof Condition){
                        List<Pair> pairList = new ArrayList<Pair>();
                        String sqlCondition = getConditions(null, ORMUtils.newList((Condition)extObject), pairList);
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
                        sqlPair = parseExpression(null, or);
                    }else if(extObject instanceof SQLPair){
                        sqlPair = (SQLPair)extObject;
                    }else if(extObject instanceof Condition){
                        List<Pair> pairList = new ArrayList<Pair>();
                        String sqlCondition = getConditions(null, ORMUtils.newList((Condition)extObject), pairList);
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
                        sqlPair = parseExpression(null, orOr);
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
                        sqlPair = parseExpression(null, orAnd);
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
                        sqlPair = parseExpression(null, orOr);
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
        String finalSql = sql.toString();
        if(finalSql.length() > 0 && values.size() > 0){
            for(Pair pair : values){
                Object value = pair.getValue();
                if(value instanceof Number){
                    finalSql = finalSql.replaceFirst("\\?", value.toString());
                }else{
                    finalSql = finalSql.replaceFirst("\\?", "'" + value.toString() + "'");
                }
            }
        }
        return finalSql;
    }

    public String getConditions(Class clazz, Expression [] expressions, List<Pair> values){
        List<String> ands = new ArrayList<String>();
        if(expressions != null) {
            for (int i = 0; i < expressions.length; i++) {
                Expression expression = expressions[i];
                if(expression == null){
                    continue;
                }
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
            if(clazz instanceof IBaseQuery){
                return parseExpression(((IBaseQuery) clazz).getTable(), null, expression);
            }else {
                IMultiQuery multiQuery = (IMultiQuery)clazz;
                Map<String, Object> tables = multiQuery.getAliasTable();
                return parseExpression(null, tables, expression);
            }
        }else{
            return parseExpression(null, null, expression);
        }
    }

    public abstract String getCaseSensitive(String name, int sensitive);

    public String getTableName(RdTable rdTable) throws ORMException{
        if(rdTable == null){
            throw new ORMException("Table annotation(RdTable) is null.");
        }
        String tableName = getCaseSensitive(rdTable.name(), rdTable.sensitive());
        if (ORMUtils.isEmpty(tableName)){
            throw new ORMException("Table 's name is empty.");
        }
        return tableName;
    }
}
