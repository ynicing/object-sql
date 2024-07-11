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
package com.objectsql.helper;

import com.objectsql.annotation.RdConvert;
import com.objectsql.annotation.RdId;
import com.objectsql.exception.ORMException;
import com.objectsql.handler.IColumnConvert;
import com.objectsql.handler.IQueryConvert;
import com.objectsql.handler.IResultSetHandler;
import com.objectsql.option.OracleOptions;
import com.objectsql.query.QueryUtils;
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.*;

/**
 * 数据库表工具类
 * Created by huangyonghua on 2016/2/16.
 */
public class SQLHelperCreator {

    //不允许删除多条记录，只能根据id删除
    /**
     * 按对象的id删除
     * @param obj
     * @return SQLHelper
     */
    public static SQLHelper delete(Object obj){

        Class clazz = obj.getClass();

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("DELETE FROM ");

        sql.append(tableName);

        Pair primaryKey = null;
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenEmpty(infoList, "Get columns cache is empty.");
        for(ColumnInfo info : infoList){
            if(info.getPrimaryKey()){
                Object fo = null;
                try {
                    Field field = info.getField();
                    field.setAccessible(true);
                    fo = field.get(obj);
                    primaryKey = handleObject(obj, info, fo);
                    break;
                } catch (Exception e) {
                    throw new ORMException("Delete table when get value error, Column[" + info.getColumnName() +
                            "], field[" + info.getName() + "]");
                }
            }
        }
        List<Pair> parameters = new ArrayList<Pair>();
        if(primaryKey != null && primaryKey.getValue() != null){
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            throw new ORMException("Delete table without primary key, Class[" + clazz.getName() + "], value[" + obj + "]");
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        helper.setPair(primaryKey);
        return helper;
    }

    /**
     *
     * @param clazz
     * @param idObject id
     * @return SQLHelper
     */
    public static SQLHelper delete(Class clazz, Object idObject){

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("DELETE FROM ");

        sql.append(tableName);

        Pair primaryKey = null;
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);

        for(ColumnInfo info : infoList){
            if(info.getPrimaryKey()){
                primaryKey = handleObject(null, info, idObject);
                break;
            }
        }
        List<Pair> parameters = new ArrayList<Pair>();
        if(primaryKey != null && idObject != null){
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            throw new ORMException("Delete table without primary key, Class[" + clazz.getName() + "], value[" + idObject + "]");
        }
        SQLHelper helper = new SQLHelper();
        helper.setPair(primaryKey);
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;

    }

    public static SQLHelper deleteBy(Class clazz, Options options, Expression[] expressions){

        String tableName = ORMUtils.getTableName(clazz);
        if(expressions == null || expressions.length == 0){
            throw new ORMException("Delete table without expresses, Class[" + clazz.getName() + "]");
        }
        StringBuffer sql = new StringBuffer("DELETE FROM " + tableName + " WHERE ");
        List<Pair> pairs = new ArrayList<Pair>();
        List<String> terms = new ArrayList<String>();
        boolean hasExpress = false;
        for(Expression expression : expressions){
            if(expression == null){
                continue;
            }
            SQLPair pair = options.parseExpression(clazz, expression);
            if(pair == null){
                continue;
            }
            terms.add(pair.getSql());
            pairs.addAll(pair.getPairs());
            hasExpress = true;
        }
        if(!hasExpress){
            throw new ORMException("Delete table without expresses, Table[" + clazz.getName() + "]");
        }
        sql.append(ORMUtils.join(terms, " AND "));
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(pairs);
        return helper;
    }

    public static SQLHelper deleteBy(Class clazz, Options options, Condition condition){
        List<Pair> pairs = new ArrayList<Pair>();
        String conditions = null;
        if(condition != null) {
            conditions = options.getConditions(clazz, ORMUtils.newList(condition), pairs);
        }
        String tableName = ORMUtils.getTableName(clazz);
        if(ORMUtils.isEmpty(conditions)){
            throw new ORMException("Delete table without conditions, Class[" + clazz.getName() + "]");
        }
        StringBuffer sql = new StringBuffer("DELETE FROM " + tableName + " WHERE " + conditions);
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(pairs);
        return helper;
    }

    public static SQLHelper updateTerms(Options option, Object obj, Condition condition, boolean updateNull, String [] nullColumns){
        List<String> ncs = new ArrayList<String>();
        if(nullColumns != null &&  nullColumns.length > 0){
            ncs.addAll(Arrays.asList(nullColumns));
        }
        Class clazz = obj.getClass();

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("UPDATE ");
        sql.append(tableName);
        sql.append(" SET ");
        List<Pair> parameters = new ArrayList<Pair>();
        List<String> sets = new ArrayList<String>();
        Pair primaryKey = null;
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
        String databaseType = DatabaseTypeHolder.get();
        SQLHelper helper = new SQLHelper();
        for(ColumnInfo info : infoList){
            Object fo = ORMUtils.getFieldValue(obj, info);
            if(info.getPrimaryKey()){// many updates when them had no ids
                helper.setIdField(info.getField());
                helper.setIdValue(fo);
                primaryKey = new Pair(info, fo);
            }
            if(info.getPrimaryKey() && updateNull){
                continue;
            }
            if(fo != null || updateNull || ncs.contains(info.getColumnName())){
                if(databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP){
                    continue;
                }
                if(fo != null){
                    Pair pair = handleObject(obj, info, fo);
                    sets.add(info.getColumnName() + " = ? ");
                    parameters.add(pair);
                }else{
                    if(updateNull || ncs.contains(info.getColumnName())){
                        sets.add(info.getColumnName() + " = NULL ");
                    }
                }
            }
        }
        sql.append(ORMUtils.join(sets, ", "));
        sql.append(" WHERE ");
        String conditions = null;
        if(condition != null) {
            conditions = option.getConditions(clazz, ORMUtils.newList(condition), parameters);
        }
        if(ORMUtils.isEmpty(conditions)) {
            throw new ORMException("Update table terms is empty, Class[" + clazz.getName() + "], value[" + obj + "]");
        }else{
            sql.append(conditions);
        }
        helper.setPair(primaryKey);
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;
    }

    /**
     * 更新 只能使用id，否则初学者不填id 全部更新了。
     * @param obj
     * @param updateNull
     * @param expressions
     * @return SQLHelper
     */
    public static SQLHelper update(Options option, Object obj, Expression [] expressions, boolean updateNull, String [] nullColumns){
        List<String> ncs = new ArrayList<String>();
        if(nullColumns != null &&  nullColumns.length > 0){
            ncs.addAll(Arrays.asList(nullColumns));
        }
        Class clazz = obj.getClass();

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("UPDATE ");
        sql.append(tableName);
        sql.append(" SET ");
        List<Pair> parameters = new ArrayList<Pair>();
        List<String> sets = new ArrayList<String>();
        Pair primaryKey = null;
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
        String databaseType = DatabaseTypeHolder.get();
        SQLHelper helper = new SQLHelper();
        for(ColumnInfo info : infoList){
            Object fo = ORMUtils.getFieldValue(obj, info);
            if(info.getPrimaryKey()){// many updates when them had no ids
                helper.setIdField(info.getField());
                helper.setIdValue(fo);
            }
            if(fo != null || updateNull || ncs.contains(info.getColumnName())){
                if(info.getPrimaryKey()){
                    primaryKey = new Pair(info, fo);
                }else {
                    if (databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP) {
                        continue;
                    }
                    if (fo != null) {
                        Pair pair = handleObject(obj, info, fo);
                        if(info.getPrimaryKey() && pair.getValue() != null){
                            helper.setIdValue(pair.getValue());
                        }
                        sets.add(info.getColumnName() + " = ? ");
                        parameters.add(pair);
                    } else {
                        if (updateNull || ncs.contains(info.getColumnName())) {
                            sets.add(info.getColumnName() + " = NULL ");
                        }
                    }
                }
            }
        }
        sql.append(ORMUtils.join(sets, ", "));
        sql.append(" WHERE ");
        if(primaryKey != null && primaryKey.getValue() != null) {
            if(expressions != null && expressions.length > 0){
                throw new ORMException("Update table with primary key, shout not add expresses, Class[" + clazz.getName() + "], value[" + obj + "]");
            }
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            if(expressions == null || expressions.length == 0) {
                throw new ORMException("Update table without primary key, but expresses is empty, Class[" + clazz.getName() + "], value[" + obj + "]");
            }else{
                List<String> terms = new ArrayList<String>();
                for(Expression expression : expressions){
                    if(expression == null){
                        continue;
                    }
                    SQLPair pair = option.parseExpression(clazz, expression);
                    if(pair == null){
                        continue;
                    }
                    terms.add(pair.getSql());
                    parameters.addAll(pair.getPairs());
                }
                sql.append(ORMUtils.join(terms, " AND "));
            }
        }

        helper.setPair(primaryKey);
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;
    }

    public static SQLHelper updateExpress(Class<?> clazz, Options options, Expression [] values, Expression [] conditions){

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("UPDATE ");
        sql.append(tableName);
        sql.append(" SET ");
        List<Pair> parameters = new ArrayList<Pair>();
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
        SQLHelper helper = new SQLHelper();

        if(values != null) {
            List<String> terms = new ArrayList<String>();
            for(Expression expression : values){
                if(expression == null){
                    continue;
                }
                SQLPair pair = options.parseExpression(clazz, expression);
                if(pair == null){
                    continue;
                }
                terms.add(pair.getSql());
                parameters.addAll(pair.getPairs());
            }
            if(!terms.isEmpty()) {
                sql.append(ORMUtils.join(terms, " , "));
            }else{
                throw new ORMException("Update values is empty, Class[" + clazz.getName() + "]");
            }
        }
        if(conditions != null) {
            List<String> terms = new ArrayList<String>();
            for(Expression expression : conditions){
                if(expression == null){
                    continue;
                }
                SQLPair pair = options.parseExpression(clazz, expression);
                if(pair == null){
                    continue;
                }
                terms.add(pair.getSql());
                parameters.addAll(pair.getPairs());
            }
            if(!terms.isEmpty()) {
                sql.append(" WHERE ");
                sql.append(ORMUtils.join(terms, " AND "));
            }
        }

        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;
    }

    public static Map<String, IDGenerator> generatorMap = new HashMap<String, IDGenerator>();

    private static Map<Class, IColumnConvert> columnConvertMap = new HashMap<Class, IColumnConvert>();

    public static SQLHelper insert(Object obj, Options options){

        Class clazz = obj.getClass();
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("INSERT INTO ");
        sql.append(tableName);
        sql.append("(");
        List<Pair> parameters = new ArrayList<Pair>();
        List<String> ps = new ArrayList<String>();
        List<String> vs = new ArrayList<String>();
        SQLHelper helper = new SQLHelper();
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
        String databaseType = DatabaseTypeHolder.get();
        for(ColumnInfo info : infoList){
            Object fo = ORMUtils.getFieldValue(obj, info);
            if(databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP){
                continue;
            }
            if(info.getPrimaryKey()){// make sure RdId exist.
                fo = createPrimaryKeyValue(options, helper, ps, vs, info, obj, fo);
            }
            if(fo != null) {
                Pair pair = handleObject(obj, info, fo);
                if(info.getPrimaryKey() && pair.getValue() != null){
                    helper.setIdValue(pair.getValue());
                }
                ps.add(info.getColumnName());
                vs.add("?");
                parameters.add(pair);
            }
        }
        if(ps.size() == 0){
            throw new ORMException("Insert into table without values, Class[" + clazz.getName() + "], value[" + obj + "]");
        }
        sql.append(ORMUtils.join(ps, ","));
        sql.append(") VALUES (");
        sql.append(ORMUtils.join(vs, ","));
        sql.append(")");

        helper.setSql(sql.toString());
        helper.setParameters(parameters);

        return helper;

    }

    private static Pair handleObject(Object obj, ColumnInfo info, Object fo) {
        Pair result = new Pair(info, fo);
        RdConvert serializer = info.getField().getAnnotation(RdConvert.class);
        if(serializer != null){
            IColumnConvert ser = columnConvertMap.get(serializer.value());
            if(ser == null) {
                try {
                    ser = serializer.value().newInstance();
                    columnConvertMap.put(serializer.value(), ser);
                } catch (InstantiationException e) {
                    throw new ORMException(e);
                } catch (IllegalAccessException e) {
                    throw new ORMException(e);
                }
            }
            if(ser != null){
                Pair kvObject = ser.setValueHandle(obj, info, fo);
                if(kvObject != null){
                    result = kvObject;
                }
            }
        }
        return result;
    }

    public static Object createPrimaryKeyValue(
        Options options,
        SQLHelper helper,
        List<String> ps,
        List<String> vs,
        ColumnInfo info,
        Object obj,
        Object fo){
        if (ORMUtils.isEmpty(fo)) {
            RdId rdId = info.getField().getAnnotation(RdId.class);
            helper.setIdField(info.getField());
            if(rdId.autoIncrement()){
                if(!ORMUtils.isEmpty(rdId.sequence()) && (options instanceof OracleOptions)){//oracle
                    ps.add(info.getColumnName());
                    vs.add(rdId.sequence() + ".nextval");
                }
            }else{
                Class generatorClass = rdId.generator();// use generator first
                if(IDGenerator.class.isAssignableFrom(generatorClass)){
                    IDGenerator generator = generatorMap.get(generatorClass.getName());
                    if(generator == null){
                        try {
                            generator = (IDGenerator)generatorClass.newInstance();
                            generatorMap.put(generatorClass.getName(), generator);
                        } catch (InstantiationException e) {
                        } catch (IllegalAccessException e) {
                        }
                    }
                    fo = generator.next(obj, info.getField().getType(), fo);
                    ORMUtils.setFieldValue(obj, info, fo);
                    helper.setIdValue(fo);
                }
            }
        } else {//值为空的时候，但是无id，需要自取了
            helper.setIdValue(fo);
        }
        return fo;
    }

    public static <S> List<SQLHelper> inserts(List<S> objs, Options options){
        List<SQLHelper> helpers = new ArrayList<SQLHelper>();
        if(objs == null || objs.isEmpty()){
            return helpers;
        }
        String insertSQL = null;
        Class clazz = null;
        for(Object obj : objs) {
            if(clazz == null) {
                clazz = obj.getClass();
            }else{
                if(!clazz.equals(obj.getClass())){
                    throw new ORMException("Error class, [" + clazz.getName() + "] but [" + obj.getClass() + "]");
                }
            }
            String tableName = ORMUtils.getTableName(clazz);
            List<Pair> parameters = new ArrayList<Pair>();
            List<String> ps = new ArrayList<String>();
            List<String> vs = new ArrayList<String>();
            SQLHelper helper = new SQLHelper();
            List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
            ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
            String databaseType = DatabaseTypeHolder.get();
            for (ColumnInfo info : infoList) {
                Object fo = ORMUtils.getFieldValue(obj, info);
                if (databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP) {
                    continue;
                }
                if (info.getPrimaryKey()) {
                    fo = createPrimaryKeyValue(options, helper, ps, vs, info, obj, fo);
                }
                Pair pair = handleObject(obj, info, fo);
                if(info.getPrimaryKey() && pair.getValue() != null){
                    helper.setIdValue(pair.getValue());
                }
                ps.add(info.getColumnName());
                vs.add("?");
                parameters.add(pair);
            }
            if(insertSQL == null) {
                StringBuffer sql = new StringBuffer("INSERT INTO ");
                sql.append(tableName);
                sql.append("(");
                sql.append(ORMUtils.join(ps, ","));
                sql.append(") VALUES (");
                sql.append(ORMUtils.join(vs, ","));
                sql.append(")");
                insertSQL = sql.toString();
            }
            helper.setSql(insertSQL);
            helper.setParameters(parameters);
            helpers.add(helper);
        }
        return helpers;
    }

    public static <S> List<SQLHelper> updates(List<S> objs, List<String> columns){
        List<SQLHelper> helpers = new ArrayList<SQLHelper>();
        if(objs == null || objs.isEmpty()){
            return helpers;
        }
        String updateSQL = null;
        Class clazz = null;
        List<ColumnInfo> selected = null;

        for(Object obj : objs) {
            if(clazz == null) {
                clazz = obj.getClass();
            }else{
                if(!clazz.equals(obj.getClass())){
                    throw new ORMException("Error class, [" + clazz.getName() + "] but [" + obj.getClass() + "]");
                }
            }
            String tableName = ORMUtils.getTableName(clazz);
            List<Pair> parameters = new ArrayList<Pair>();
            SQLHelper helper = new SQLHelper();
            ColumnInfo primaryKey = null;
            if(selected == null){
                List<String> ps = new ArrayList<String>();
                selected = new ArrayList<ColumnInfo>();
                List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
                ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
                String databaseType = DatabaseTypeHolder.get();
                for (ColumnInfo info : infoList) {
                    if (databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP) {
                        continue;
                    }
                    if(info.getPrimaryKey()){
                        primaryKey = info;
                        continue;
                    }
                    if(columns != null){
                        if(columns.contains(info.getColumnName())){
                            selected.add(info);
                        }
                    }else{
                        selected.add(info);
                    }
                }
                ORMUtils.whenTrue(primaryKey == null, "No primary key for table : " + clazz);
                ORMUtils.whenTrue(selected.isEmpty(), "No valid column for table : " + clazz);
                StringBuffer sql = new StringBuffer("UPDATE ");
                sql.append(tableName);
                sql.append(" SET ");
                for (ColumnInfo info : selected) {
                    if (databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP) {
                        continue;
                    }
                    ps.add(String.format(" %s = ? ", info.getColumnName()));
                }
                selected.add(primaryKey);
                sql.append(ORMUtils.join(ps, ","));
                sql.append(String.format(" WHERE %s = ? ", primaryKey.getColumnName()));
                updateSQL = sql.toString();
            }
            for (ColumnInfo info : selected) {
                Object fo = ORMUtils.getFieldValue(obj, info);
                Pair pair = handleObject(obj, info, fo);
                parameters.add(pair);
            }
            helper.setSql(updateSQL);
            helper.setParameters(parameters);
            helpers.add(helper);
        }
        return helpers;
    }

    /**
     * 允许获取 id，匹配，唯一等值
     * 有id根据id获取，其他根据列 等值获取
     * @param obj
     * @return SQLHelper
     */
    public static SQLHelper get(Object obj, String ... names){

        Class clazz = obj.getClass();

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("SELECT ");
        String nameStr = ORMUtils.join(names, ",");
        if(ORMUtils.isEmpty(nameStr)){
            sql.append("*");
        }else{
            sql.append(nameStr);
        }
        sql.append(" FROM ");
        sql.append(tableName);

        List<Pair> parameters = new ArrayList<Pair>();
        List<String> ps = new ArrayList<String>();
        Pair primaryKey = null;

        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
        for(ColumnInfo info : infoList) {
            Object fo = ORMUtils.getFieldValue(obj, info);
            if (fo != null) {
                Pair pair = handleObject(obj, info, fo);
                if (info.getPrimaryKey()) {
                    primaryKey = pair;
                    break;
                } else {
                    ps.add(info.getColumnName() + " = ? ");
                    parameters.add(pair);
                }
            }
        }
        if(primaryKey != null) {
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.clear();
            parameters.add(primaryKey);
        }else{
            if(ps.size() > 0){
                sql.append(" WHERE " + ORMUtils.join(ps, " AND "));
            }
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;

    }

    public static SQLHelper query(Options options, Class<?> clazz, Expression [] expressions){
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT * FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        String conditions = options.getConditions(clazz, expressions, values);
        if (!ORMUtils.isEmpty(conditions)) {
            sql.append(" WHERE " + conditions);
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);
        return helper;
    }



    public static SQLHelper queryCountExpress(Options options, Class<?> clazz, Expression ... expressions){
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        String conditions = options.getConditions(clazz, expressions, values);
        if (!ORMUtils.isEmpty(conditions)) {
            sql.append(" WHERE " + conditions);
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);
        return helper;
    }

    public static SQLHelper queryCount(Options options, Class<?> clazz, Condition condition){
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        String conditions = options.getConditions(clazz, ORMUtils.newList(condition), values);
        if (!ORMUtils.isEmpty(conditions)) {
            sql.append(" WHERE " + conditions);
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);
        return helper;
    }

    /**
     * 只允许ID
     * @param id
     * @param clazz
     * @return SQLHelperCreator
     */
    public static SQLHelper get(Class clazz, Object id){

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("SELECT * ");
        sql.append("FROM ");
        sql.append(tableName);
        sql.append(" WHERE ");
        Pair primaryKey = null;
        List<Pair> parameters = new ArrayList<Pair>();
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        ORMUtils.whenTrue(infoList == null, "Get columns cache is empty.");
        for(ColumnInfo info : infoList){
            if(info.getPrimaryKey() && id != null){
                primaryKey = handleObject(null, info, id);
                break;
            }
        }
        if(primaryKey == null){
            throw new ORMException("Select table without primary key, Class[" + clazz.getName() + "], value[" + id + "]");
        }
        sql.append(primaryKey.getColumn() + " = ? ");
        parameters.add(primaryKey);
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;
    }

    private  static <T> T parse(IQueryConvert queryConvert, Object value, Class distClass) throws SQLException{
        Object object = null;
        if(queryConvert != null){
            object = queryConvert.queryValueHandle(value, distClass);
        }
        return (T)object;
    }

    private  static KV parseMap(IQueryConvert queryConvert, Map<String, Object> tempMap, Object value, Class distClass) throws SQLException{
        KV object = null;
        if(queryConvert != null){
            object = queryConvert.queryValueHandleMap(tempMap, value, distClass);
        }
        return object;
    }

    private  static <T> T parseObject(IColumnConvert columnConvert, Object result, Object value, ColumnInfo columnInfo) throws SQLException{
        Object object = null;
        if(columnConvert != null){
            object = columnConvert.getValueHandle(result, columnInfo, value);
        }
        return (T)object;
    }

    private  static <T> T parseObject(IQueryConvert queryConvert, Object result, Object value, ColumnInfo columnInfo) throws SQLException{
        Object object = null;
        if(queryConvert != null){
            object = queryConvert.queryValueHandle(result, columnInfo, value);
        }
        return (T)object;
    }

    public static <T> T newClass(Class clazz, ResultSet rs, IResultSetHandler resultSetHandler)
            throws IllegalAccessException, SQLException {
        return newClass(clazz, rs, null, resultSetHandler);
    }

    public static <T> T newClass(Class clazz, ResultSet rs, IQueryConvert queryConvert, IResultSetHandler resultSetHandler)
            throws IllegalAccessException, SQLException {
        ResultSetMetaData metaMap = rs.getMetaData();
        T t = null;
        DataType type = DataType.getDataType(clazz.getSimpleName());

        Object object = rs.getObject(1);
        if(type != DataType.MAP && type != DataType.UNKNOWN){
            Object parser = parse(queryConvert, object, clazz);
            if(parser != null){
                return (T)parser;
            }
        }
        //String, Map, Bean
        switch (type){
            case STRING:
                if(object != null) {
                    t = (T) object.toString();
                }
                break;
            case INTEGER:
                t = (T)new Integer(rs.getInt(1));
                break;
            case LONG:
                t = (T)new Long(rs.getLong(1));
                break;
            case FLOAT:
                t = (T)new Float(rs.getFloat(1));
                break;
            case DOUBLE:
                t = (T)new Double(rs.getDouble(1));
                break;
            case SHORT:
                t = (T)new Short(rs.getShort(1));
                break;
            case BYTE:
                t = (T)new Byte(rs.getByte(1));
                break;
            case BOOLEAN:
                t = (T)new Boolean(rs.getBoolean(1));
                break;
            case MAP :
                Map<String, Object> tempMap = new HashMap<String, Object>();
                for(int i = 1; i <= metaMap.getColumnCount(); i++){
                    Object obj = rs.getObject(i);

                    KV kv = parseMap(queryConvert, tempMap, object, clazz);
                    if(kv != null){
                        tempMap.put(kv.getKey(), kv.getValue());
                        continue;
                    }
                    kv = resultSetHandler.parseMap(metaMap, i, obj, rs);
                    if(kv != null) {
                        tempMap.put(kv.getKey(), kv.getValue());
                    }
                }
                t = (T)tempMap;
                break;
            case UNKNOWN:// Object.
                ResultSetMetaData meta = rs.getMetaData();
                Map<String, Object> temp = new HashMap<String, Object>();
                for(int i = 1; i <= meta.getColumnCount(); i++){// orderTest - ORDERTEST ---> ORDER_NUM
                    String label = meta.getColumnLabel(i);
                    String name = meta.getColumnName(i);
                    Object value = rs.getObject(label);
                    String tmp = QueryUtils.displayNameOrAsName(label, name);
                    temp.put(tmp, value);
                    temp.put(tmp.toLowerCase(Locale.ROOT), value);
                    temp.put(tmp.toUpperCase(Locale.ROOT), value);
                    temp.put(label.toLowerCase(Locale.ROOT), value);
                    temp.put(label.toUpperCase(Locale.ROOT), value);
                }
                //如果是bigdeicmal？
                try {
                    t = (T)clazz.newInstance();
                }catch (Exception e){
                    e.printStackTrace();
                }
                List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
                for(ColumnInfo info : infoList){
                    handleInfo(rs, queryConvert, resultSetHandler, t, temp, info);
                }
                List<ColumnInfo> fields = ORMUtils.getExtendFields(clazz);
                for (ColumnInfo info : fields){
                    handleInfo(rs, queryConvert, resultSetHandler, t, temp, info);
                }
                break;
            default:
                break;
        }

        return t;

    }

    private static <T> void handleInfo(ResultSet rs, IQueryConvert queryConvert, IResultSetHandler resultSetHandler, T t, Map<String, Object> temp, ColumnInfo info) throws SQLException {
        Object obj = null;
        if(info.getColumnName() != null){
            obj = temp.get(info.getColumnName());
            if(obj == null){
                obj = temp.get(info.getColumnName().toUpperCase(Locale.ROOT));
            }
            if(obj == null){
                obj = temp.get(info.getColumnName().toLowerCase(Locale.ROOT));
            }
        }else{
            obj = temp.get(info.getName());
            if(obj == null){
                obj = temp.get(info.getName().toUpperCase(Locale.ROOT));
            }
            if(obj == null){
                obj = temp.get(info.getName().toLowerCase(Locale.ROOT));
            }
        }

        if(obj != null){
            //mysql tinyint(1) getObject会导致返回  boolean对象；修改为tinyint(2)或者如下兼容
            if("integer".equalsIgnoreCase(info.getType()) && (obj instanceof Boolean)){
                obj = rs.getInt(info.getColumnName());
            }
            //若有优先column( column处理完给query)，若无直接处理query
            boolean done = false;
            Object kvObject = null;
            RdConvert deserializer = info.getField().getAnnotation(RdConvert.class);
            if(deserializer != null){
                IColumnConvert des = columnConvertMap.get(deserializer.value());
                if(des == null) {
                    try {
                        des = deserializer.value().newInstance();
                        columnConvertMap.put(deserializer.value(), des);
                    } catch (InstantiationException e) {
                        throw new ORMException(e);
                    } catch (IllegalAccessException e) {
                        throw new ORMException(e);
                    }
                }
                if(des != null){
                    kvObject = parseObject(des, t, obj, info);
                    if(kvObject != null){
                        Object tempKvObject = parseObject(queryConvert,t, obj, info);
                        if(tempKvObject != null){
                            ORMUtils.setFieldValue(t, info, tempKvObject);
                        }else {
                            ORMUtils.setFieldValue(t, info, kvObject);
                        }
                        done = true;
                    }
                }
            }
            if(!done){
                kvObject = parseObject(queryConvert,t, obj, info);
                if(kvObject != null){
                    ORMUtils.setFieldValue(t, info, kvObject);
                    done = true;
                }
            }
            if(!done){
                resultSetHandler.handle(t, info, obj, rs);
            }
        }
    }


    public static void setParameter(Options options, PreparedStatement ps, List<Pair> objects, Connection connection) throws SQLException{
        if(options == null){
            return;
        }
        String databaseType = DatabaseTypeHolder.get();
        //ps.setObject(); 是否可以统一使用
        for(int i = 0; i < objects.size(); i++){
            Pair pair = objects.get(i);
            options.setParameter(ps, connection, databaseType, i, pair);
        }
    }


}
