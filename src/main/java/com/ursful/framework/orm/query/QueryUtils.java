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


import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.ISQLScript;
import com.ursful.framework.orm.support.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryUtils {

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

//	private static String columnToFieldName(String name){
//		if(name == null || name.length() == 0){
//			return null;
//		}
//		String [] names = name.split("_");
//		StringBuffer sb = new StringBuffer(names[0].toLowerCase());
//		for(int i = 1; i < names.length; i++){
//			String n = names[i];
//			sb.append(n.substring(0,1).toUpperCase());
//			sb.append(n.substring(1));
//		}
//		return sb.toString();
//	}

//	private static Field getFieldOrSuper(String name, Class<?> clazz){
//		Class<?> tmp = clazz;
//		Field field = null;
//		while(tmp != null){
//			try {
//				field = tmp.getDeclaredField(columnToFieldName(name));
//			} catch (Exception e) {
//				tmp = tmp.getSuperclass();
//				continue;
//			}
//			break;
//		}
//		return field;
//	}



	public static String getOrders(List<Order> orders){
        List<String> temp = new ArrayList<String>();
        if(orders == null){
            return null;
        }
		for(Order order : orders){
            temp.add(parseColumn(order.getColumn()) + " " + order.getOrder());
		}
        if(temp.size() > 0){
            return ORMUtils.join(temp, ",");
        }
		return null;
	}


	public static String getGroups(List<Column> columns){
		List<String> temp = new ArrayList<String>();
		for(Column column : columns){
			temp.add(parseColumn(column));
		}
		if(temp.size() > 0){
			return ORMUtils.join(temp, ",");
		}
		return null;
	}




	public static String parseColumn(Column column){

        if(column == null){
            throw new RuntimeException("QUERY_SQL_COLUMN_IS_NULL this column is null");
        }
        if(column.getName() == null){
            throw new RuntimeException("QUERY_SQL_NAME_IS_NULL this column name is null.");
        }

		StringBuffer sb = new StringBuffer();

        if(column.getFunction() != null){


            if(column.getType() != null && column.getValue() != null){
                if(column.getValue() instanceof Integer || column.getValue() instanceof Long){
                    sb.append(column.getFunction() + "(");
                    if(column.getAlias() != null && !"".equals(column.getAlias())){
                        sb.append(column.getAlias() + ".");
                    }
                    sb.append(column.getName());
                    sb.append(")");
                    sb.append(column.getType().getOperator() + column.getValue().toString());
                }else if(column.getValue() instanceof Column){
                    sb.append(column.getFunction() + "(");
                    if(column.getAlias() != null && !"".equals(column.getAlias())){
                        sb.append(column.getAlias() + ".");
                    }
                    sb.append(column.getName());

                    sb.append(sb.append(column.getType().getOperator() + parseColumn((Column)column.getValue())));

                    sb.append(")");

                }

            }else{

                sb.append(column.getFunction() + "(");
                if(column.getAlias() != null && !"".equals(column.getAlias())){
                    sb.append(column.getAlias() + ".");
                }
                sb.append(column.getName());

                sb.append(")");

            }


        }else{
            if(column.getAlias() != null && !"".equals(column.getAlias())){
                sb.append(column.getAlias() + ".");
            }
            sb.append(column.getName());
            if(column.getType() != null && column.getValue() != null){
                sb.append(column.getType().getOperator() + column.getValue().toString());
            }
        }
        if(column.getAsName() != null){
            sb.append(" " + column.getAsName());
        }
		return sb.toString();
	}



    //同一张表怎么半？ select * from test t1, test t2

    public static String getConditions(List<Condition> cds, List<Pair> values){
        if(cds == null){
            return null;
        }
        List<String> ands = new ArrayList<String>();

        for(Condition condition : cds){
            List<String> ors = new ArrayList<String>();
            List<Expression> orExpressions = condition.getOrExpressions();
            for(int i = 0; i < orExpressions.size(); i++){
                Expression expression = orExpressions.get(i);
                SQLPair sqlPair = parseExpression(expression);
                if(sqlPair != null){
                    ors.add(sqlPair.getSql());
                    if(sqlPair.getPairs() != null) {//column = column
                        values.addAll(sqlPair.getPairs());
                    }
                }
            }

            List<List<Expression>> orAnds = condition.getOrAnds();
            for(int i = 0; i < orAnds.size(); i++){
                List<Expression> oras = orAnds.get(i);
                List<String> temp = new ArrayList<String>();
                for(int j = 0; j < oras.size(); j++){
                    Expression expression = oras.get(j);
                    SQLPair sqlPair = parseExpression(expression);
                    if(sqlPair != null){
                        temp.add(sqlPair.getSql());
                        if(sqlPair.getPairs() != null) {
                            values.addAll(sqlPair.getPairs());
                        }
                    }
                }
                if(!temp.isEmpty()) {
                    ors.add("(" + ORMUtils.join(temp, " AND ") + ")");
                }
            }

            if(ors.size() > 0){
                ands.add("(" + ORMUtils.join(ors, " OR ") + ") ");
            }
            List<Expression> andExpressions = condition.getAndExpressions();
            for(int i = 0; i < andExpressions.size(); i++){
                Expression expression = andExpressions.get(i);
                SQLPair sqlPair = parseExpression(expression);
                if(sqlPair != null){
                    ands.add(sqlPair.getSql());
                    if(sqlPair.getPairs() != null) {
                        values.addAll(sqlPair.getPairs());
                    }
                }
            }

        }
        if(ands.isEmpty()){
            return null;
        }
        return ORMUtils.join(ands, " AND ");
    }

    public static SQLHelper parseScript(ISQLScript script, Object [] objects, boolean updateNull){
        StringBuffer sql = new StringBuffer();
        if(script.columns().length != objects.length){
            return null;
        }
        switch (script.type()){
            case TABLE_SAVE:
                sql.append("INSERT INTO " + script.table());
                break;
            case TABLE_UPDATE:
                sql.append("UPDATE " + script.table());
                break;
            case TABLE_DELETE:
                sql.append("DELETE FROM " + script.table());
        }

        return null;
    }

    public static String getConditions(Express [] expresses, List<Pair> values){
        if(expresses == null){
            return null;
        }
        List<String> ands = new ArrayList<String>();
        for(int i = 0; i < expresses.length; i++){
            Expression expression = expresses[i].getExpression();
            SQLPair sqlPair = parseExpression(expression);
            if(sqlPair != null){
                ands.add(sqlPair.getSql());
                if(sqlPair.getPairs() != null) {
                    values.addAll(sqlPair.getPairs());
                }
            }
        }

        if(ands.isEmpty()){
            return null;
        }

        return ORMUtils.join(ands, " AND ");
    }

    public static SQLPair parseExpression(Expression expression){
        SQLPair sqlPair = null;
        if(expression == null || expression.getLeft() == null){
            return sqlPair;
        }
        String conditionName = parseColumn(expression.getLeft());
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
                return sqlPair;
            }
        }
        if(ORMUtils.isEmptyObject(conditionValue)){
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
                    sqlPair = new SQLPair(" " + conditionName + " = ?", new Pair(conditionValue));
                    break;
                case CDT_NotEqual:
                    sqlPair = new SQLPair(" " + conditionName + " != ?", new Pair(conditionValue));
                    break;
                case CDT_More:
                    sqlPair = new SQLPair(" "+ conditionName + " > ?", new Pair(conditionValue));
                    break;
                case CDT_MoreEqual:
                    sqlPair = new SQLPair(" "+ conditionName + " >= ?", new Pair(conditionValue));
                    break;
                case CDT_Less:
                    sqlPair = new SQLPair(" "+ conditionName + " < ?", new Pair(conditionValue));
                    break;
                case CDT_LessEqual:
                    sqlPair = new SQLPair(" "+ conditionName + " <= ?", new Pair(conditionValue));
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
                                    values.add(new Pair(obj));
                                }
                            }else{
                                names.add("?");
                                values.add(new Pair(obj));
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

//    public static TableInfo getTableInfoFromClass(Class clazz) throws CommonException{
//        TableInfo info = new TableInfo();
//        info.setName(clazz.getSimpleName());
//        RdTable rt = (RdTable)clazz.getAnnotation(RdTable.class);
//        if(rt == null){
//            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_CLASS_HAS_NOT_TABLE, "this bean has not map table");
//        }
//        info.setTableName(rt.name());
//        info.setClazz(clazz);
//        Map<String, ColumnInfo> columns = new HashMap<String, ColumnInfo>();
//        for(Field f : clazz.getDeclaredFields()){
//            RdColumn rc = (RdColumn)f.getAnnotation(RdColumn.class);
//            if(rc != null){
//                ColumnInfo in = new ColumnInfo();
//                in.setColumnName(rc.name());
//                in.setName(f.getName());
//                in.setField(f);
//                in.setDataType(DataType.getDataType(f.getType().getSimpleName()));
//                columns.put(in.getColumnName(), in);
//            }
//        }
//        if(columns.isEmpty()){
//            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_CLASS_HAS_NOT_ANY_COLUMN, "this bean has not map columns");
//        }
//        info.setColumns(columns);
//        return info;
//    }

    private static AtomicInteger count = new AtomicInteger(0);


//    public static QueryInfo doQuery(IQuery query, Page page, DatabaseType databaseType) throws CommonException {
//
//
//	}


    public static void main(String[] args) {

    }






}
