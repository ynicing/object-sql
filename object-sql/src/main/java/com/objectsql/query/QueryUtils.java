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
package com.objectsql.query;


import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;

import java.util.*;

public class QueryUtils {

    public static Date plusDate235959(Date date){
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR, 23);
        calendar.set(Calendar.MINUTE, 59);
        calendar.set(Calendar.SECOND, 59);
        calendar.set(Calendar.MILLISECOND,999);
        return calendar.getTime();
    }

    public static void setMultiOrderAlias(MultiOrder multiOrder, String alias){
        if(multiOrder != null && !ORMUtils.isEmpty(alias)){
            setOrdersAlias(multiOrder.getOrders(), alias);
        }
    }

    public static void setOrdersAlias(List<Order> orders, String alias){
        if(orders != null&& !ORMUtils.isEmpty(alias)){
            for(Order order : orders){
                setColumnAlias(order.getColumn(), alias);
            }
        }
    }

    public static void setConditionsAlias(List<Condition> conditions, String alias){
        if(conditions != null && !ORMUtils.isEmpty(alias)){
            for(Condition condition : conditions){
                setConditionAlias(condition, alias);
            }
        }
    }
    public static void setConditionAlias(Condition condition, String alias){
        if(condition != null && !ORMUtils.isEmpty(alias)){
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
                        }else if(extObject instanceof Condition){
                            setConditionAlias((Condition)extObject, alias);
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
        if(expressions != null && !ORMUtils.isEmpty(alias)){
            for(Expression expression : expressions){
                setExpressionAlias(expression, alias);
            }
        }
    }

    public static void setExpressionAlias(Expression expression, String alias){
        if(expression != null && !ORMUtils.isEmpty(alias)){
            setColumnAlias(expression.getLeft(), alias);
            if(expression.getValue() instanceof Column){
                setColumnAlias((Column)expression.getValue(), alias);
            }
        }
    }
    public static void setColumnsAlias(List<Column> columns, String alias){
        if(columns != null && !ORMUtils.isEmpty(alias)){
            for(Column column : columns){
                setColumnAlias(column, alias);
            }
        }
    }

    public static void setColumnAlias(Column column, String alias){
        if(column != null && !ORMUtils.isEmpty(alias)){
            if(ORMUtils.isEmpty(column.getAlias())) {
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
        if(displayName.toUpperCase(Locale.ROOT).equals(columnName.toUpperCase(Locale.ROOT))){
            String [] names = columnName.split("_");
            if(names.length == 1){
                if(isUpperOrLowerCase(columnName)){
                    return columnName;
                }
            }
            StringBuffer sb = new StringBuffer(names[0].toLowerCase(Locale.ROOT));
            for(int i = 1; i < names.length; i++){
                String n = names[i];
                sb.append(n.substring(0,1).toUpperCase(Locale.ROOT));
                sb.append(n.substring(1).toLowerCase(Locale.ROOT));
            }
            return sb.toString();
        }else{
            if(displayName.contains("_")){
                String [] names = displayName.split("_");
                StringBuffer sb = new StringBuffer(names[0].toLowerCase(Locale.ROOT));
                for(int i = 1; i < names.length; i++){
                    String n = names[i];
                    if(n.length() > 0) {
                        sb.append(n.substring(0, 1).toUpperCase(Locale.ROOT));
                        sb.append(n.substring(1).toLowerCase(Locale.ROOT));
                    }
                }
                return sb.toString();
            }else {
                return displayName.toLowerCase(Locale.ROOT);
            }
        }
    }

    public static String displayName(String columnName){
        String [] names = columnName.split("_");
        if(names.length == 1){
            if(isUpperOrLowerCase(columnName)){
                return columnName;
            }
        }
        StringBuffer sb = new StringBuffer(names[0].toLowerCase(Locale.ROOT));
        for(int i = 1; i < names.length; i++){
            String n = names[i];
            if(n.length() > 0) {
                sb.append(n.substring(0, 1).toUpperCase(Locale.ROOT));
                sb.append(n.substring(1).toLowerCase(Locale.ROOT));
            }
        }
        return sb.toString();
    }

    public static <T,R> String[] getColumns(LambdaQuery<T, R>... lambdaQueries){
        String [] columns = null;
        if(lambdaQueries != null){
            columns = new String[lambdaQueries.length];
            for(int i = 0; i < lambdaQueries.length; i++){
                columns[i] = lambdaQueries[i].getColumnName();
            }
        }
        return columns;
    }


    public static <T,R> String getColumn(LambdaQuery<T, R> lambdaQuery){
        String  column = null;
        if(lambdaQuery != null){
            column = lambdaQuery.getColumnName();
        }
        return column;
    }

    public static <T,R> ColumnInfo getColumnInfo(LambdaQuery<T, R> lambdaQuery){
        ColumnInfo  column = null;
        if(lambdaQuery != null){
            column = lambdaQuery.getColumnInfo();
        }
        return column;
    }
}
