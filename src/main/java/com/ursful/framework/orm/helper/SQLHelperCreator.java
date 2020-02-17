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
package com.ursful.framework.orm.helper;

import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.validation.DataBinder;

import java.io.*;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;
import java.util.Date;


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
        Assert.notEmpty(infoList, "Get columns cache is empty.");
        for(ColumnInfo info : infoList){
            if(info.getPrimaryKey()){
                Object fo = null;
                try {
                    Field field = info.getField();
                    field.setAccessible(true);
                    fo = field.get(obj);
                    primaryKey = new Pair(info, fo);
                    break;
                } catch (Exception e) {
                    throw new RuntimeException("Delete table when get value error, Column[" + info.getColumnName() +
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
            throw new RuntimeException("Delete table without primary key, Class[" + clazz.getName() + "], value[" + obj + "]");
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
                primaryKey = new Pair(info, idObject);
                break;
            }
        }
        List<Pair> parameters = new ArrayList<Pair>();
        if(primaryKey != null && idObject != null){
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            throw new RuntimeException("Delete table without primary key, Class[" + clazz.getName() + "], value[" + idObject + "]");
        }
        SQLHelper helper = new SQLHelper();
        helper.setPair(primaryKey);
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;

    }

    public static SQLHelper deleteBy(Class clazz, Options options, Express[] expresses){

        String tableName = ORMUtils.getTableName(clazz);
        if(expresses == null || expresses.length == 0){
            throw new RuntimeException("Delete table without expresses, Class[" + clazz.getName() + "]");
        }
        StringBuffer sql = new StringBuffer("DELETE FROM " + tableName + " WHERE ");
        List<Pair> pairs = new ArrayList<Pair>();
        List<String> terms = new ArrayList<String>();
        boolean hasExpress = false;
        for(Express express : expresses){
            if(express == null){
                continue;
            }
            SQLPair pair = QueryUtils.parseExpression(options, clazz, express.getExpression());
            terms.add(pair.getSql());
            pairs.addAll(pair.getPairs());
            hasExpress = true;
        }
        if(!hasExpress){
            throw new RuntimeException("Delete table without expresses, Table[" + clazz.getName() + "]");
        }
        sql.append(ORMUtils.join(terms, " AND "));
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(pairs);
        return helper;
    }

    public static SQLHelper deleteBy(Class clazz, Options options, Terms terms){
        List<Pair> pairs = new ArrayList<Pair>();
        String conditions = null;
        if(terms != null) {
            Condition condition = terms.getCondition();
            conditions = QueryUtils.getConditions(options, clazz, ORMUtils.newList(condition), pairs);
        }
        String tableName = ORMUtils.getTableName(clazz);
        if(StringUtils.isEmpty(conditions)){
            throw new RuntimeException("Delete table without conditions, Class[" + clazz.getName() + "]");
        }
        StringBuffer sql = new StringBuffer("DELETE FROM " + tableName + " WHERE " + conditions);
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(pairs);
        return helper;
    }

    public static SQLHelper updateTerms(Options option, Object obj, Terms terms, boolean updateNull, String [] nullColumns){
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
        Assert.notNull(infoList, "Get columns cache is empty.");
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
                    sets.add(info.getColumnName() + " = ? ");
                    Pair pair = new Pair(info, fo);
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
        if(terms != null) {
            Condition condition = terms.getCondition();
            conditions = QueryUtils.getConditions(option, clazz, ORMUtils.newList(condition), parameters);
        }
        if(StringUtils.isEmpty(conditions)) {
            throw new RuntimeException("Update table terms is empty, Class[" + clazz.getName() + "], value[" + obj + "]");
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
     * @param expresses
     * @return SQLHelper
     */
    public static SQLHelper update(Options option, Object obj, Express [] expresses, boolean updateNull, String [] nullColumns){
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
        Assert.notNull(infoList, "Get columns cache is empty.");
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
                        sets.add(info.getColumnName() + " = ? ");
                        Pair pair = new Pair(info, fo);
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
            if(expresses != null && expresses.length > 0){
                throw new RuntimeException("Update table with primary key, shout not add expresses, Class[" + clazz.getName() + "], value[" + obj + "]");
            }
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            if(expresses == null || expresses.length == 0) {
                throw new RuntimeException("Update table without primary key, but expresses is empty, Class[" + clazz.getName() + "], value[" + obj + "]");
            }else{
                List<String> terms = new ArrayList<String>();
                for(Express express : expresses){
                    if(express == null){
                        continue;
                    }
                    SQLPair pair = QueryUtils.parseExpression(option, clazz, express.getExpression());
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

    public static SQLHelper updateExpress(Class<?> clazz, Options options, Express [] values, Express [] conditions){

        String tableName = ORMUtils.getTableName(clazz);

        StringBuffer sql = new StringBuffer("UPDATE ");
        sql.append(tableName);
        sql.append(" SET ");
        List<Pair> parameters = new ArrayList<Pair>();
        List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
        Assert.notNull(infoList, "Get columns cache is empty.");
        SQLHelper helper = new SQLHelper();

        if(values != null) {
            List<String> terms = new ArrayList<String>();
            for(Express express : values){
                if(express == null){
                    continue;
                }
                SQLPair pair = QueryUtils.parseExpression(options, clazz, express.getExpression());
                terms.add(pair.getSql());
                parameters.addAll(pair.getPairs());
            }
            if(!terms.isEmpty()) {
                sql.append(ORMUtils.join(terms, " , "));
            }else{
                throw new RuntimeException("Update values is empty, Class[" + clazz.getName() + "]");
            }
        }
        if(conditions != null) {
            List<String> terms = new ArrayList<String>();
            for(Express express : conditions){
                if(express == null){
                    continue;
                }
                SQLPair pair = QueryUtils.parseExpression(options, clazz, express.getExpression());
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


    public static SQLHelper insert(Object obj){

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
        Assert.notNull(infoList, "Get columns cache is empty.");
        String databaseType = DatabaseTypeHolder.get();
        for(ColumnInfo info : infoList){
            Object fo = ORMUtils.getFieldValue(obj, info);
            if(databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP){
                continue;
            }
            if(info.getPrimaryKey()){
                helper.setIdField(info.getField());
                if(StringUtils.isEmpty(fo)){
                    if(String.class.getSimpleName().equals(info.getType())){
                        fo = UUID.randomUUID().toString();
                        ORMUtils.setFieldValue(obj, info, fo);
                        helper.setIdValue(fo);
                    }
                }else{//值为空的时候，但是无id，需要自取了
                    helper.setIdValue(fo);
                }
            }
            if(fo != null) {
                ps.add(info.getColumnName());
                vs.add("?");
                Pair pair = new Pair(info, fo);
                parameters.add(pair);
            }
        }
        if(ps.size() == 0){
            throw new RuntimeException("Insert into table without values, Class[" + clazz.getName() + "], value[" + obj + "]");
        }
        sql.append(ORMUtils.join(ps, ","));
        sql.append(") VALUES (");
        sql.append(ORMUtils.join(vs, ","));
        sql.append(")");

        helper.setSql(sql.toString());
        helper.setParameters(parameters);

        return helper;

    }

    public static <S> List<SQLHelper> inserts(List<S> objs){
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
                    throw new RuntimeException("Error class, [" + clazz.getName() + "] but [" + obj.getClass() + "]");
                }
            }
            String tableName = ORMUtils.getTableName(clazz);
            List<Pair> parameters = new ArrayList<Pair>();
            List<String> ps = new ArrayList<String>();
            List<String> vs = new ArrayList<String>();
            SQLHelper helper = new SQLHelper();
            List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
            Assert.notNull(infoList, "Get columns cache is empty.");
            String databaseType = DatabaseTypeHolder.get();
            for (ColumnInfo info : infoList) {
                Object fo = ORMUtils.getFieldValue(obj, info);
                if (databaseType != null && databaseType.contains("SERVER") && info.getColumnType() == ColumnType.TIMESTAMP) {
                    continue;
                }
                if (info.getPrimaryKey()) {
                    helper.setIdField(info.getField());
                    if (StringUtils.isEmpty(fo)) {
                        if (String.class.getSimpleName().equals(info.getType())) {
                            fo = UUID.randomUUID().toString();
                            ORMUtils.setFieldValue(obj, info, fo);
                            helper.setIdValue(fo);
                        }
                    } else {//值为空的时候，但是无id，需要自取了
                        helper.setIdValue(fo);
                    }
                }
                ps.add(info.getColumnName());
                vs.add("?");
                Pair pair = new Pair(info, fo);
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
        if(StringUtils.isEmpty(nameStr)){
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
        Assert.notNull(infoList, "Get columns cache is empty.");
        for(ColumnInfo info : infoList) {
            Object fo = ORMUtils.getFieldValue(obj, info);
            if (fo != null) {
                if (info.getPrimaryKey()) {
                    primaryKey = new Pair(info, fo);
                    break;
                } else {
                    ps.add(info.getColumnName() + " = ? ");
                    Pair temp = new Pair(info, fo);
                    parameters.add(temp);
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

    public static SQLHelper query(Options options, Class<?> clazz, Express [] expresses){
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT * FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        String conditions = QueryUtils.getConditions(options, clazz, expresses, values);
        if (!StringUtils.isEmpty(conditions)) {
            sql.append(" WHERE " + conditions);
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);
        return helper;
    }



    public static SQLHelper queryCountExpress(Options options, Class<?> clazz, Express ... expresses){
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        String conditions = QueryUtils.getConditions(options, clazz, expresses, values);
        if (!StringUtils.isEmpty(conditions)) {
            sql.append(" WHERE " + conditions);
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);
        return helper;
    }

    public static SQLHelper queryCount(Options options, Class<?> clazz, Terms terms){
        String tableName = ORMUtils.getTableName(clazz);
        StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        String conditions = QueryUtils.getConditions(options, clazz, ORMUtils.newList(terms.getCondition()), values);
        if (!StringUtils.isEmpty(conditions)) {
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
        Assert.notNull(infoList, "Get columns cache is empty.");
        for(ColumnInfo info : infoList){
            if(info.getPrimaryKey() && id != null){
                primaryKey = new Pair(info, id);
                break;
            }
        }
        if(primaryKey == null){
            throw new RuntimeException("Select table without primary key, Class[" + clazz.getName() + "], value[" + id + "]");
        }
        sql.append(primaryKey.getColumn() + " = ? ");
        parameters.add(primaryKey);
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;
    }

    public static <T> T newClass(Class clazz, ResultSet rs)
            throws IllegalAccessException, SQLException {

        T t = null;
        DataType type = DataType.getDataType(clazz.getSimpleName());
        //String, Map, Bean
        switch (type){
            case STRING:
                Object object = rs.getObject(1);
                if(object != null) {
                    t = (T) object.toString();
                }
                break;
            case MAP :
                ResultSetMetaData metaMap = rs.getMetaData();
                Map<String, Object> tempMap = new HashMap<String, Object>();
                for(int i = 1; i <= metaMap.getColumnCount(); i++){
                    Object obj = rs.getObject(i);
                    if(obj != null) {
                        if (obj instanceof Timestamp) {
                            obj = new Date(((Timestamp) obj).getTime());
                        }else if(obj instanceof java.sql.Date){
                            obj = new Date(((java.sql.Date)obj).getTime());
                        }else if(metaMap.getPrecision(i) == 15) {
                            if((obj instanceof  Long) ){
                                obj = new Date((Long)obj);
                            }else if(obj instanceof BigDecimal){
                                obj = new Date(((BigDecimal)obj).longValue());
                            }
                        }if(obj instanceof Clob){
                            StringBuffer sb = new StringBuffer();
                            Clob clob = (Clob) obj;
                            if(clob != null){
                                Reader reader = clob.getCharacterStream();
                                BufferedReader br = new BufferedReader(reader);
                                String s = null;
                                try {
                                    while((s = br.readLine()) != null){
                                        sb.append(s);
                                    }
                                } catch (Exception e) {
                                }
                            }
                            obj = sb.toString();
                        }else if(obj instanceof Blob){
                            Blob blob = (Blob) obj;
                            InputStream stream = blob.getBinaryStream();
                            try {
                                byte[] temp = new byte[(int)blob.length()];
                                stream.read(temp);
                                stream.close();
                                obj = new String(temp, "utf-8");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }else if(obj instanceof byte[]){
                            try {
                                obj = new String((byte[])obj, "utf-8");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }

                        }else if(obj instanceof BigDecimal){
                            obj = ((BigDecimal) obj).toPlainString();
                        }else if(obj instanceof Double || obj instanceof Long || obj instanceof Float){
                            obj = new BigDecimal(obj.toString()).toPlainString();
                        }
                    }
                    String key = QueryUtils.displayNameOrAsName(metaMap.getColumnLabel(i), metaMap.getColumnName(i));
                    tempMap.put(key, obj);
                }
                t = (T)tempMap;
                break;
            case UNKNOWN:
                try {
                    t = (T)clazz.newInstance();
                }catch (Exception e){
                    e.printStackTrace();
                }

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
                List<ColumnInfo> infoList = ORMUtils.getColumnInfo(clazz);
                for(ColumnInfo info : infoList){
                    Object obj = temp.get(info.getColumnName());
                    if(obj == null){
                        obj = temp.get(info.getColumnName().toUpperCase(Locale.ROOT));
                    }
                    if(obj == null) {
                        obj = temp.get(info.getName());
                    }
                    if(obj != null){
                        setFieldValue(t, info, obj);
                    }
                }
                List<ColumnInfo> fields = ORMUtils.getExtendFields(clazz);
                for (ColumnInfo info : fields){
                    Object obj = temp.get(info.getName());
                    if(obj == null){
                        obj = temp.get(info.getName().toUpperCase(Locale.ROOT));
                    }
                    if(obj == null){
                        obj = temp.get(info.getName().toLowerCase(Locale.ROOT));
                    }
                    if(obj != null){
                        setFieldValue(t, info, obj);
                    }
                }
                break;
            case INTEGER:
                t = (T)new Integer(rs.getInt(1));
        }

        return t;

    }


    public static void setFieldValue(Object object, ColumnInfo info, Object value) throws IllegalAccessException, SQLException {
        Object obj = null;
        DataType type = DataType.getDataType(info.getType());
        if(value != null){
            switch (type) {
                case BINARY:
                    if(value instanceof byte[]){
                        obj = value;
                    }else if(value instanceof Blob){
                        Blob blob = (Blob) value;
                        InputStream stream = blob.getBinaryStream();
                        byte[] temp = null;
                        try {
                            temp = new byte[(int)blob.length()];
                            stream.read(temp);
                            stream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        obj = temp;
                    }else{
                        obj = value;
                    }
                    break;
                case STRING:
                    if(value instanceof Clob){
                        StringBuffer sb = new StringBuffer();
                        Clob clob = (Clob) value;
                        if(clob != null){
                            Reader reader = clob.getCharacterStream();
                            BufferedReader br = new BufferedReader(reader);
                            String s = null;
                            try {
                                while((s = br.readLine()) != null){
                                    sb.append(s);
                                }
                            } catch (Exception e) {
                            }
                        }
                        obj = sb.toString();
                    }else if(value instanceof Blob){
                        Blob blob = (Blob) value;
                        InputStream stream = blob.getBinaryStream();
                        try {
                            byte[] temp = new byte[(int)blob.length()];
                            stream.read(temp);
                            stream.close();
                            obj = new String(temp, "utf-8");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }else if(value instanceof byte[]){
                        try {
                            obj = new String((byte[])value, "utf-8");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }else{
                        obj = value;
                    }
                    break;
                case DATE:
                    if(value instanceof Long) {
                        Long ts = (Long) value;
                        if (ts != null && ts.longValue() > 0) {
                            obj = new Date(ts);
                        }
                    }else if(value instanceof java.sql.Date){
                        java.sql.Date tmp = (java.sql.Date) value;
                        obj = new Date(tmp.getTime());
                    }else if(value instanceof java.sql.Timestamp){
                        Timestamp ts =  (Timestamp)value;
                        if(ts != null) {
                            obj = new Date(ts.getTime());
                        }
                    }else if(value instanceof BigDecimal){
                        BigDecimal ts =  (BigDecimal)value;
                        if(ts != null) {
                            obj = new Date(ts.longValue());
                        }
                    }else{
                        //  throw new RuntimeException("not support");
                    }
                    break;
                default:
                    if(value instanceof BigDecimal){
                        value = ((BigDecimal) value).toPlainString();
                    }else if(value instanceof Double || value instanceof Long || value instanceof Float){
                        value = new BigDecimal(value.toString()).toPlainString();
                    }
                    DataBinder binder = new DataBinder(info.getField(), info.getName());
                    obj = binder.convertIfNecessary(value.toString(), info.getField().getType());
            }
        }
        if(obj != null){
            info.getField().setAccessible(true);
            info.getField().set(object, obj);
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
            Object obj = pair.getValue();
            ColumnType columnType = pair.getColumnType();
            DataType type =  DataType.getDataType(pair.getType());
            options.setParameter(ps, connection, databaseType, i, obj, columnType, type);
        }
    }


}
