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

import com.ursful.framework.orm.support.DebugHolder;
import com.ursful.framework.orm.annotation.RdColumn;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

/**
 * 类名：ORMUtils
 * 创建者：huangyonghua
 * 日期：2017-11-11 15:14
 * 版权：厦门维途信息技术有限公司 Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class ORMUtils {


    public static String DEFAUT_DATASOURCE = "dataSource";

    private static boolean debug = true;

    private static boolean isTrim = false;

    public static boolean isTrim(){
        return isTrim;
    }

    public static void setTrim(boolean trim){
        isTrim = trim;
    }

    public static void setAllDebug(boolean d){
        debug = d;
    }

    public static void setDebug(boolean debug){
        DebugHolder.set(debug + "");
    }

    public static boolean getDebug(){//true
        if(debug) {
            String _debug = DebugHolder.get();
            if (_debug != null && "false".equals(_debug)) {
                return false;
            }
            return true;
        }
        return false;
    }

    private static Map<Class, Map<String, String>> fieldColumnCache = new HashMap<Class, Map<String, String>>();
    private static Map<Class, Map<String, String>> columnFieldCache = new HashMap<Class, Map<String, String>>();
    private static Map<Class, List<String>> fieldCache = new HashMap<Class, List<String>>();
    private static Map<Class, List<String>> columnCache = new HashMap<Class, List<String>>();

    public static Map<String, String> getFieldColumn(Class<?> clazz){
        if(fieldColumnCache.containsKey(clazz)){
            return fieldColumnCache.get(clazz);
        }

        Map<String, String> temp = new HashMap<String, String>();
        Field [] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column != null){
                temp.put(field.getName(), column.name());
            }
        }
        Class<?> tmp = clazz.getSuperclass();
        while (tmp != null){
            temp.putAll(getFieldColumn(tmp));
            tmp = tmp.getSuperclass();
        }
        fieldColumnCache.put(clazz, temp);
        return temp;
    }

    public static Map<String, String> getColumnField(Class<?> clazz){
        if(columnFieldCache.containsKey(clazz)){
            return columnFieldCache.get(clazz);
        }
        Map<String, String> temp = new HashMap<String, String>();
        Field [] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column != null){
                temp.put(column.name(), field.getName());
            }
        }
        Class<?> tmp = clazz.getSuperclass();
        while (tmp != null){
            temp.putAll(getColumnField(tmp));
            tmp = tmp.getSuperclass();
        }
        columnFieldCache.put(clazz, temp);
        return temp;
    }

    public static List<String> getFields(Class<?> clazz){
        if(fieldCache.containsKey(clazz)){
            return fieldCache.get(clazz);
        }
        List<String> temp = new ArrayList<String>();
        Field [] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column != null){
                temp.add(field.getName());
            }
        }
        Class<?> tmp = clazz.getSuperclass();
        while (tmp != null){
            temp.addAll(getFields(tmp));
            tmp = tmp.getSuperclass();
        }
        fieldCache.put(clazz, temp);
        return temp;
    }

    public static List<String> getColumns(Class<?> clazz){
        if(columnCache.containsKey(clazz)){
            return columnCache.get(clazz);
        }
        List<String> temp = new ArrayList<String>();
        Field [] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            RdColumn column = field.getAnnotation(RdColumn.class);
            if(column != null){
                temp.add(column.name());
            }
        }
        Class<?> tmp = clazz.getSuperclass();
        while (tmp != null){
            temp.addAll(getColumns(tmp));
            tmp = tmp.getSuperclass();
        }
        columnCache.put(clazz, temp);
        return temp;
    }

    public static <T> List<T> newList(T ... ts){
        List<T> temp = new ArrayList<T>();
        if(ts == null){
            return null;
        }
        for(T t : ts){
            temp.add(t);
        }
        return temp;
    }

    public static boolean isEmpty(String value){
        if(value == null){
            return true;
        }
        if("".equals(value)){
            return true;
        }
        return false;
    }

    public static boolean isTheSameClass(Class<?> thisClass, Class<?> clazz){
        Type[] ts = clazz.getGenericInterfaces();
        if(ts.length > 0) {
            Type [] types = ((ParameterizedType) ts[0]).getActualTypeArguments();
            if(types.length > 0){
                Class<?> tp = (Class<?>) types[0];
                if(thisClass.isAssignableFrom(tp)){
                    return true;
                }else if(Object.class.getName().equals(tp.getName())){
                    return true;
                }
            }

        }
        return false;
    }

    public static boolean isEmptyObject(Object object){
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
    public static String join(List<String> words, String key){
        StringBuffer sb = new StringBuffer();
        for(String word : words){
            if(sb.length() == 0){
                sb.append(word);
            }else{
                sb.append(key + word);
            }
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println(getColumns(ORMUtils.class));
    }

}
