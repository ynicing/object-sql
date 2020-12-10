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
package com.ursful.framework.orm.utils;

import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdId;
import com.ursful.framework.orm.support.DebugHolder;
import com.ursful.framework.orm.annotation.RdObject;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.support.ColumnInfo;
import com.ursful.framework.orm.support.ColumnType;
import org.springframework.core.annotation.AnnotationUtils;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ORMUtils {

    //开启debug模式，
    private static boolean debug = false;
    private static boolean isTrim = false;
    private static boolean lowerCaseSql = false;
    private static int runningMode = -1;

    public static void setRunningMode(int runningMode){
        runningMode = runningMode;
    }

    public static int getRunningMode(){
        return runningMode;
    }

    public static void enableLowerCaseSql(boolean lowerCase){
        lowerCaseSql = lowerCase;
    }

    public static boolean isLowerCaseSql(){
        return lowerCaseSql;
    }

    public static String convertSQL(String sql){
        if(sql != null){
            if(lowerCaseSql){
                return sql.toLowerCase(Locale.ROOT);
            }else{
                return sql.toUpperCase(Locale.ROOT);
            }
        }
        return sql;
    }

    public static boolean isTrim(){
        return isTrim;
    }

    public static void enableTrim(boolean trim){
        isTrim = trim;
    }

    public static void enableDebug(boolean d){
        debug = d;
    }

    public static void enableCurrentThreadDebug(boolean debug){
        DebugHolder.set(debug + "");
    }

    public static boolean getDebug(){//true
        String _debug = DebugHolder.get();
        if (_debug != null) {
            return "true".equals(_debug);
        }
        return debug;
    }

    private static Map<Class, List<ColumnInfo>> columnInfoCache = new ConcurrentHashMap<Class, List<ColumnInfo>>();

    private static Map<Class, String> tableNameCache = new ConcurrentHashMap<Class, String>();
    private static Map<Class, RdTable> rdTableCache = new ConcurrentHashMap<Class, RdTable>();

    public static String getTableName(Class clazz){
        if(tableNameCache.containsKey(clazz)){
            return tableNameCache.get(clazz);
        }
        RdTable table = AnnotationUtils.findAnnotation(clazz, RdTable.class);
        if(table == null){
            throw new RuntimeException("Table not found Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();
        rdTableCache.put(clazz, table);
        tableNameCache.put(clazz, tableName);
        return tableName;
    }

    public static RdTable getRdTable(Class clazz){
        if(rdTableCache.containsKey(clazz)){
            return rdTableCache.get(clazz);
        }
        RdTable table = AnnotationUtils.findAnnotation(clazz, RdTable.class);
        if(table == null){
            return null;
        }
        String tableName = table.name();
        rdTableCache.put(clazz, table);
        tableNameCache.put(clazz, tableName);
        return table;
    }

    public static void setFieldValue(Object object, ColumnInfo info, Object value){
        setFieldValue(object, info.getField(), value);
    }

    public static void setFieldValue(Object object, Field field, Object value){
        if(object == null || value == null || field == null){
            return;
        }
        try {
            field.setAccessible(true);
            field.set(object, value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Set value to [" + object + "] error, value[" + value + "]");
        }
    }

    public static void setFieldNullValue(Object object, Field field){
        if(object == null || field == null){
            return;
        }
        try {
            field.setAccessible(true);
            field.set(object, null);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Set value to [" + object + "] error, value[NULL]");
        }
    }

    public static Object getFieldValue(Object object, Field field){
        if(object == null || field == null){
            return null;
        }
        Object result = null;
        try {
            field.setAccessible(true);
            result = field.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Get value from [" + object + "] error!");
        }
        return result;
    }

    public static Object getFieldValue(Object object, ColumnInfo info){
        return getFieldValue(object, info.getField());
    }

    public static void whenEmpty(Object object, String message){
        if(isEmpty(object)){
            throw new RuntimeException(message);
        }
    }

    public static void whenTrue(boolean flag, String message){
        if(flag){
            throw new RuntimeException(message);
        }
    }

    private static List<ColumnInfo> analyze(Class<?> clazz){
        List<ColumnInfo> infoList = new ArrayList<ColumnInfo>();
        List<Field> fields = getDeclaredFields(clazz);
        List<String> fieldNames = new ArrayList<String>();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column != null){//针对RdColumn
                if(fieldNames.contains(field.getName())){
                    continue;
                }
                fieldNames.add(field.getName());
                ColumnInfo info = new ColumnInfo();
                info.setField(field);
                info.setOrder(column.order());
                info.setName(field.getName());
                info.setColumnName(column.name());
                info.setColumnType(column.type());
                info.setType(field.getType().getSimpleName());
                RdId id = field.getAnnotation(RdId.class);
                if(id != null){
                    info.setPrimaryKey(true);
                    infoList.add(0, info);
                }else{
                    infoList.add(info);
                }
            }
        }
        infoList.sort(new Comparator<ColumnInfo>() {
            @Override
            public int compare(ColumnInfo o1, ColumnInfo o2) {
                return o1.getOrder() - o2.getOrder();
            }
        });
        columnInfoCache.put(clazz, infoList);
        return infoList;
    }


    public static <T> List<T> newList(T ... ts){
        List<T> temp = new ArrayList<T>();
        if(ts == null){
            return temp;
        }
        for(T t : ts){
            temp.add(t);
        }
        return temp;
    }

    private static Type[] getTypes(Type type){
        Type [] types = null;
        if(type instanceof ParameterizedType){
            types = ((ParameterizedType) type).getActualTypeArguments();
        }else{
            Type[] temp = ((Class)type).getGenericInterfaces();
            if(temp.length > 0) {
                types = getTypes(temp[0]);
            }
        }
        return types;
    }

    public static boolean isTheSameClass(Class<?> thisClass, Class<?> clazz){
        RdObject object = AnnotationUtils.findAnnotation(clazz, RdObject.class);
        if(object != null){
            Class<?> [] classes = object.classes();
            if(classes != null){
                for(Class<?> c : classes){
                    if (thisClass.isAssignableFrom(c)) {
                        return true;
                    }
                }
            }
            String [] patterns = object.patterns();
            if(patterns != null){
                for(String pattern : patterns){
                    if (thisClass.getName().startsWith(pattern)) {
                        return true;
                    }
                    Pattern p = Pattern.compile(pattern);
                    Matcher matcher = p.matcher(thisClass.getName());
                    if (matcher.find()) {
                        return true;
                    }
                }
            }
            String [] prefixes = object.prefixes();
            if(prefixes != null){
                for(String prefix : prefixes){
                    if (thisClass.getName().startsWith(prefix)) {
                        return true;
                    }
                }
            }
            return false;
        }
        Type[] ts = clazz.getGenericInterfaces();
        if(ts.length > 0) {
            try {
                Type [] types = getTypes(ts[0]);
                if (types.length > 0) {
                    Class<?> tp = (Class<?>) types[0];
                    if (thisClass.isAssignableFrom(tp)) {
                        return true;
                    } else if (Object.class.getName().equals(tp.getName())) {
                        return true;
                    }
                }
            }catch (Exception e){
                return false;
            }
        }
        return false;
    }

    public static boolean isEmpty(Object object){
        if(object == null){
            return true;
        }
        if("".equals(object.toString())){
            return true;
        }
        if(object instanceof Collection){
            return ((Collection)object).isEmpty();
        }
        return false;
    }

    public static String join(String [] words, String key){
        StringBuffer sb = new StringBuffer();
        if(words != null) {
            for (String word : words) {
                if (isEmpty(word)) {
                    continue;
                }
                if (sb.length() == 0) {
                    sb.append(word);
                } else {
                    sb.append(key + word);
                }
            }
        }
        return sb.toString();
    }

    public static String join(List<String> words, String key){
        StringBuffer sb = new StringBuffer();
        if(words != null) {
            for (String word : words) {
                if (isEmpty(word)) {
                    continue;
                }
                if (sb.length() == 0) {
                    sb.append(word);
                } else {
                    sb.append(key + word);
                }
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(getColumns(ORMUtils.class));
    }

    public static Map<String, ColumnType> getColumnType(Class<?> clazz){
        Map<String, ColumnType> temp = new HashMap<String, ColumnType>();
        List<ColumnInfo> infoList = columnInfoCache.get(clazz);
        if(infoList == null){
            infoList = analyze(clazz);
        }
        if(infoList != null){
            for(ColumnInfo info: infoList){
                temp.put(info.getColumnName(), info.getColumnType());
            }
        }
        return temp;
    }

    public static Map<String, String> getFieldColumn(Class<?> clazz){
        Map<String, String> temp = new HashMap<String, String>();
        List<ColumnInfo> infoList = columnInfoCache.get(clazz);
        if(infoList == null){
            infoList = analyze(clazz);
        }
        if(infoList != null){
            for(ColumnInfo info: infoList){
                temp.put(info.getName(), info.getColumnName());
            }
        }
        return temp;
    }

    public static String getFieldByColumn(Class<?> clazz, String columnName){
        Map<String, String> map = getColumnField(clazz);
        return map.get(columnName);
    }

    public static Map<String, String> getColumnField(Class<?> clazz){
        Map<String, String> temp = new HashMap<String, String>();
        List<ColumnInfo> infoList = columnInfoCache.get(clazz);
        if(infoList == null){
            infoList = analyze(clazz);
        }
        if(infoList != null){
            for(ColumnInfo info: infoList){
                temp.put(info.getColumnName(), info.getName());
            }
        }
        return temp;
    }

    public static void copyPropertiesWhenUpdateNull(Object from, Object to, String ... columns){
        if(from == null || to == null){
            return;
        }
        List<String> list = new ArrayList<String>();
        if(columns != null && columns.length > 0){
            list.addAll(Arrays.asList(columns)) ;
        }
        if(from.getClass() == to.getClass()){
            Class clazz = to.getClass();
            List<ColumnInfo> columnInfos = getColumnInfo(clazz);
            ORMUtils.whenTrue(columnInfos == null, "Get columns cache is empty.");
            for(ColumnInfo info : columnInfos){
                Object fromValue = getFieldValue(from, info);
                if(ORMUtils.isEmpty(fromValue)){//因为属性
                    if(list.contains(info.getColumnName())) {
                        setFieldNullValue(to, info.getField());
                    }
                }else{
                    setFieldValue(to, info, fromValue);
                }
            }
        }
    }

    public static List<String> getFields(Class<?> clazz){
        List<String> temp = new ArrayList<String>();
        List<ColumnInfo> infoList = columnInfoCache.get(clazz);
        if(infoList == null){
            infoList = analyze(clazz);
        }
        if(infoList != null){
            for(ColumnInfo info: infoList){
                temp.add(info.getName());
            }
        }
        return temp;
    }

    public static List<ColumnInfo> getColumnInfo(Class<?> clazz){
        List<ColumnInfo> infoList = columnInfoCache.get(clazz);
        if(infoList == null){
            infoList = analyze(clazz);
        }
        return infoList;
    }

    public static List<String> getColumns(Class<?> clazz){
        List<String> temp = new ArrayList<String>();
        List<ColumnInfo> infoList = columnInfoCache.get(clazz);
        if(infoList == null){
            infoList = analyze(clazz);
        }
        if(infoList != null) {
            for(ColumnInfo info: infoList){
                temp.add(info.getColumnName());
            }
        }
        return temp;
    }

    public static List<Field> getDeclaredFields(Class<?> clazz){
        List<Field> temp = new ArrayList<Field>();
        if(clazz == null){
            return temp;
        }
        Field [] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column != null){
                temp.add(field);
            }
        }
        Class<?> tmp = clazz.getSuperclass();
        temp.addAll(getDeclaredFields(tmp));
        return temp;
    }

    private static Map<Class, List<ColumnInfo>> extendFieldCache = new ConcurrentHashMap<Class, List<ColumnInfo>>();

    public static List<ColumnInfo> getExtendFields(Class<?> clazz){
        List<ColumnInfo> infoList = extendFieldCache.get(clazz);
        if(infoList == null){
            List<Field> fields = getDeclaredExtendFields(clazz);
            infoList = new ArrayList<ColumnInfo>();
            for(Field field : fields){
                ColumnInfo info = new ColumnInfo();
                info.setField(field);
                info.setName(field.getName());
                info.setType(field.getType().getSimpleName());
                infoList.add(info);
            }
            extendFieldCache.put(clazz, infoList);
        }
        return infoList;
    }

    public static List<Field> getDeclaredExtendFields(Class<?> clazz){
        List<Field> temp = new ArrayList<Field>();
        if(clazz == null){
            return temp;
        }
        Field [] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column == null){
                temp.add(field);
            }
        }
        Class<?> tmp = clazz.getSuperclass();
        temp.addAll(getDeclaredExtendFields(tmp));
        return temp;
    }
}
