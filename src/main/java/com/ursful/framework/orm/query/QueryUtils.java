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
package com.ursful.framework.orm.query;


import com.ursful.framework.orm.IMultiQuery;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.orm.support.*;
import org.springframework.util.StringUtils;

import java.util.*;

public class QueryUtils {

    public static void setMultiOrderAlias(MultiOrder multiOrder, String alias){
        if(multiOrder != null && !StringUtils.isEmpty(alias)){
            setOrdersAlias(multiOrder.getOrders(), alias);
        }
    }

    public static void setOrdersAlias(List<Order> orders, String alias){
        if(orders != null&& !StringUtils.isEmpty(alias)){
            for(Order order : orders){
                setColumnAlias(order.getColumn(), alias);
            }
        }
    }

    public static void setConditionsAlias(List<Condition> conditions, String alias){
        if(conditions != null && !StringUtils.isEmpty(alias)){
            for(Condition condition : conditions){
                setConditionAlias(condition, alias);
            }
        }
    }
    public static void setConditionAlias(Condition condition, String alias){
        if(condition != null && !StringUtils.isEmpty(alias)){
            List<ConditionObject> exts = condition.getConditions();
            for(ConditionObject ext : exts){
                if(ext == null){
                    continue;
                }
                Object extObject = ext.getObject();
                switch (ext.getType()){
                    case AND:
                    case OR:
                        if(extObject instanceof Expression) {
                            setExpressionAlias((Expression)extObject, alias);
                        }
                        break;
                    case AND_OR:
                    case OR_OR:
                    case OR_AND:
                        Expression[] list = (Expression[])extObject;
                        setExpressionsAlias(list, alias);
                        break;
                }
            }
        }
    }

    public static void setExpressionsAlias(Expression[] expressions, String alias){
        if(expressions != null && !StringUtils.isEmpty(alias)){
            for(Expression expression : expressions){
                setExpressionAlias(expression, alias);
            }
        }
    }

    public static void setExpressionAlias(Expression expression, String alias){
        if(expression != null && !StringUtils.isEmpty(alias)){
            setColumnAlias(expression.getLeft(), alias);
            if(expression.getValue() instanceof Column){
                setColumnAlias((Column)expression.getValue(), alias);
            }
        }
    }
    public static void setColumnsAlias(List<Column> columns, String alias){
        if(columns != null && !StringUtils.isEmpty(alias)){
            for(Column column : columns){
                setColumnAlias(column, alias);
            }
        }
    }

    public static void setColumnAlias(Column column, String alias){
        if(column != null && !StringUtils.isEmpty(alias)){
            if(StringUtils.isEmpty(column.getAlias())) {
                column.setAlias(alias);
            }
            if(column.getValue() instanceof  Column){
                setColumnAlias((Column) column.getValue(), alias);
            }
        }
    }

    public static boolean isUpperOrLowerCase(String columnName){
        if(ORMUtils.isEmpty(columnName)){
            return false;
        }
        boolean isUpper = false;
        boolean isLower = false;
        for(int i = 0; i < columnName.length(); i++){
            char c = columnName.charAt(i);
            if(!isUpper && Character.isUpperCase(c)){
                isUpper = true;
            }
            if(!isLower && Character.isLowerCase(c)){
                isLower = true;
            }
            if(isUpper && isLower){
                break;
            }
        }
        return isUpper && isLower;
    }

    public static String displayNameOrAsName(String displayName, String columnName){
        if(displayName.toUpperCase().equals(columnName.toUpperCase())){
            String [] names = columnName.split("_");
            if(names.length == 1){
                if(isUpperOrLowerCase(columnName)){
                    return columnName;
                }
            }
            StringBuffer sb = new StringBuffer(names[0].toLowerCase());
            for(int i = 1; i < names.length; i++){
                String n = names[i];
                sb.append(n.substring(0,1).toUpperCase());
                sb.append(n.substring(1).toLowerCase());
            }
            return sb.toString();
        }else{
            if(displayName.contains("_")){
                String [] names = displayName.split("_");
                StringBuffer sb = new StringBuffer(names[0].toLowerCase());
                for(int i = 1; i < names.length; i++){
                    String n = names[i];
                    sb.append(n.substring(0,1).toUpperCase());
                    sb.append(n.substring(1).toLowerCase());
                }
                return sb.toString();
            }else {
                return displayName;
            }
        }
    }

	public static String getOrders(Options options, List<Order> orders){
        List<String> temp = new ArrayList<String>();
        if(orders != null){
            for(Order order : orders){
                temp.add(parseColumn(options, order.getColumn()) + " " + order.getOrder());
            }
        }
        String result = ORMUtils.join(temp, ",");
		return result;
	}


	public static String getGroups(Options options, List<Column> columns){
		List<String> temp = new ArrayList<String>();
		for(Column column : columns){
			temp.add(parseColumn(options, column));
		}
        String result = ORMUtils.join(temp, ",");
        return result;
	}


	public static String parseColumn(Options options, Column column){

        if(column == null){
            throw new RuntimeException("QUERY_SQL_COLUMN_IS_NULL this column is null");
        }
        if(column.getName() == null){
            throw new RuntimeException("QUERY_SQL_NAME_IS_NULL this column name is null.");
        }

		StringBuffer sb = new StringBuffer();

        if(!ORMUtils.isEmpty(column.getFunction())){
            if(!ORMUtils.isEmpty(column.getType())){
                String result = getColumnWithOperatorAndFunction(options, column);
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
                String result = getColumnWithOperator(options, column);
                sb.append(result);
            }else{
                String aliasName = column.getName();
                if(!ORMUtils.isEmpty(column.getAlias())){
                    aliasName = column.getAlias() + "." + aliasName;
                }
                if(StringUtils.isEmpty(column.getFormat())){
                    sb.append(aliasName);
                }else{
                    sb.append(String.format(column.getFormat(), aliasName));
                }
            }
        }
        // operator out of function as name is invalid
        if(!ORMUtils.isEmpty(column.getAsName())){
            sb.append(" AS " + column.getAsName());
        }
		return sb.toString();
	}


    private static String getColumnWithOperator(Options options, Column column) {
        String name = ORMUtils.isEmpty(column.getAlias())? column.getName():column.getAlias()  + "." + column.getName();
        String value = null;
        if(column.getValue() instanceof Column){
            value = "(" + parseColumn(options,(Column)column.getValue()) + ")";
        }else{
            if(column.getValue() != null) {
                value = column.getValue().toString();
            }
        }
        String result = null;
        if(options != null) {
            result = options.getColumnWithOperator(column.getType(), name, value);
        }
        if(result == null){
            result = name + column.getType().getOperator() + value;
        }
        return result;
    }


    private static String getColumnWithOperatorAndFunction(Options options, Column column) {
        boolean inFunction = column.getOperatorInFunction();
        String name = ORMUtils.isEmpty(column.getAlias())? column.getName():column.getAlias()  + "." + column.getName();
        String value = null;
        if(column.getValue() instanceof Column){
            value = "(" + parseColumn(options, (Column)column.getValue()) + ")";
        }else{
            if(column.getValue() != null) {
                value = column.getValue().toString();
            }
        }

        String type = DatabaseTypeHolder.get();
        String function = column.getFunction();
        OperatorType operatorType = column.getType();
        String result = options.getColumnWithOperatorAndFunction(function, inFunction, operatorType, name, value);
        return result;
    }
    //同一张表怎么半？ select * from test t1, test t2


    // a and b or (c or d ) or (c and d)
    public static String getConditions(Options options, Object queryOrClass, List<Condition> cds, List<Pair> values){
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
                                sqlPair = parseExpression(options, queryOrClass, and);
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
                                sqlPair = parseExpression(options, queryOrClass, or);
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
                                sqlPair = parseExpression(options, queryOrClass, orOr);
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
                                sqlPair = parseExpression(options, queryOrClass, orAnd);
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
                                sqlPair = parseExpression(options, queryOrClass, orOr);
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

    public static String getConditions(Options options, Class clazz, Express [] expresses, List<Pair> values){
        List<String> ands = new ArrayList<String>();
        if(expresses != null) {
            for (int i = 0; i < expresses.length; i++) {
                Express express = expresses[i];
                if(express == null){
                    continue;
                }
                Expression expression = express.getExpression();
                SQLPair sqlPair = parseExpression(options, clazz, expression);
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

    public static SQLPair parseExpression(Options options, Object clazz, Expression expression){
        if(clazz instanceof Class){
            return parseExpression(options, (Class)clazz, null, expression);
        }else if(clazz instanceof IQuery){
            IQuery query = (IQuery)clazz;
            return parseExpression(options, query.getTable(), query.getAliasTable(), expression);
        }else{
            return parseExpression(null,null, null, expression);
        }
    }

    public static SQLPair parseExpression(Options options, Class clazz, Map<String,Class<?>> clazzes, Expression expression){

//    public static SQLPair parseExpression(Expression expression){
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

        String conditionName = parseColumn(options, expression.getLeft());
        Object conditionValue = expression.getValue();

        boolean isTrim = ORMUtils.isTrim();
        if(isTrim && (conditionValue instanceof String)){
            conditionValue = ((String) conditionValue).trim();
        }
        if(conditionValue != null ) {
            if (conditionValue instanceof Column) {
                String valueSQL = parseColumn(options, (Column) conditionValue);
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
                return sqlPair;
            }
        }
        if(ORMUtils.isEmpty(conditionValue)){
            switch (expression.getType()) {
                case CDT_IS_NULL:
                    sqlPair = new SQLPair(" "+ conditionName + " IS NULL ");
                    break;
                case CDT_IS_NOT_NULL:
                    sqlPair = new SQLPair(" "+ conditionName + " IS NOT NULL ");
                    break;
                default:
                    break;
            }
        }else{
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

    public static void main(String[] args) {

    }

}
