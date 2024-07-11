package com.objectsql.support;

import com.objectsql.utils.ORMUtils;
import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.Map;

public interface LambdaQuery<T, R> extends java.util.function.Function<T, R>, Serializable {

    long serialVersionUID = 1L;

    default String getColumnName() {
        try {
            Method method = getClass().getDeclaredMethod("writeReplace");
            method.setAccessible(true);
            SerializedLambda lambda = (SerializedLambda) method.invoke(this);
            String methodName = lambda.getImplMethodName();
            String first = null;
            String last = null;
            if(methodName.startsWith("is")){
                first = methodName.substring(2, 3);
                last = methodName.substring(3);
            }else if(methodName.startsWith("get")){
                first = methodName.substring(3, 4);
                last = methodName.substring(4);
            }
            if(first != null && last != null) {
                String fieldName = first.toLowerCase(Locale.ROOT) + last;
                Class clazz = Class.forName(lambda.getImplClass().replace("/", "."));
                Map<String, String> map = ORMUtils.getFieldColumn(clazz);
                return map.get(fieldName);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    default ColumnInfo getColumnInfo() {
        try {
            Method method = getClass().getDeclaredMethod("writeReplace");
            method.setAccessible(true);
            SerializedLambda lambda = (SerializedLambda) method.invoke(this);
            String methodName = lambda.getImplMethodName();
            String first = null;
            String last = null;
            if(methodName.startsWith("is")){
                first = methodName.substring(2, 3);
                last = methodName.substring(3);
            }else if(methodName.startsWith("get")){
                first = methodName.substring(3, 4);
                last = methodName.substring(4);
            }
            if(first != null && last != null) {
                String fieldName = first.toLowerCase(Locale.ROOT) + last;
                Class clazz = Class.forName(lambda.getImplClass().replace("/", "."));
                Map<String, ColumnInfo> map = ORMUtils.getFieldColumnInfo(clazz);
                return map.get(fieldName);
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        } catch (NoSuchMethodException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }
}