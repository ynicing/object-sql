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
package com.objectsql.handler;

import com.objectsql.exception.ORMException;
import com.objectsql.support.DataType;
import com.objectsql.annotation.RdColumn;
import com.objectsql.query.QueryUtils;
import com.objectsql.support.ColumnInfo;
import com.objectsql.support.KV;
import org.springframework.validation.DataBinder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.TimeZone;

public class DefaultResultSetHandler implements IResultSetHandler{

    @Override
    public String decode(Class clazz, ColumnInfo info, Object value, ResultSet resultSet) {
        if(info != null && info.getField() != null){
            RdColumn column  = info.getField().getAnnotation(RdColumn.class);
            if(column != null){
                return column.coding();
            }
        }
        return "UTF-8";
    }

    @Override
    public KV parseMap(ResultSetMetaData metaData, int index, Object obj, ResultSet rs) throws SQLException{
        if(obj != null) {
//            if (obj instanceof Timestamp) {
//                obj = new Date(((Timestamp) obj).getTime());
//            }else if(obj instanceof java.sql.Date){
//                obj = new Date(((java.sql.Date)obj).getTime());
//            }else if(metaData.getPrecision(index) == 15 && metaData.getScale(index) == 0) {
//                if((obj instanceof  Long) ){
//                    obj = new Date((Long)obj);
//                }else if(obj instanceof BigDecimal){
//                    obj = new Date(((BigDecimal)obj).longValue());
//                }
//            }else
            if(obj instanceof Clob){
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
                    obj = new String(temp, decode(null, null, obj, rs));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }else if(obj instanceof byte[]){
                try {
                    obj = new String((byte[])obj, decode(null, null, obj, rs));
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
//            else if(obj instanceof BigDecimal){
//                obj = ((BigDecimal) obj).toPlainString();
//            }else if(obj instanceof Double || obj instanceof Long || obj instanceof Float){
//                obj = new BigDecimal(obj.toString()).toPlainString();
//            }
            String key = QueryUtils.displayNameOrAsName(metaData.getColumnLabel(index), metaData.getColumnName(index));
            return new KV(key, obj);
        }
        return null;
    }

    @Override
    public void handle(Object object, ColumnInfo info, Object value, ResultSet rs) throws SQLException {
        if(object == null){
            return;
        }
        Class clazz = object.getClass();
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
                            String coding = decode(clazz, info, value, rs);
                            obj = new String(temp, coding);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }else if(value instanceof byte[]){
                        try {
                            String coding = decode(clazz, info, value, rs);
                            obj = new String((byte[])value, coding);
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
                    }else if(value instanceof java.sql.Time){
                        obj = (Date)value;
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
                    }else if(value instanceof LocalDateTime) {
                        LocalDateTime ldt = (LocalDateTime) value;
                        int hour = TimeZone.getDefault().getRawOffset()/1000/3600;
                        obj = new Date((long)(ldt.toInstant(ZoneOffset.ofHours(hour)).toEpochMilli() * 1.0));
                    }else{
                        throw new ORMException("Handle date value not support now.");
                    }
                    break;
                default:
                    if(value instanceof BigDecimal){
                        value = ((BigDecimal) value).toPlainString();
                    }else if(value instanceof Double
                            || value instanceof Long
                            || value instanceof Short
                            || value instanceof Byte
                            || value instanceof Boolean
                            || value instanceof Float){
                        value = value.toString();
                    }
                    DataBinder binder = new DataBinder(info.getField(), info.getName());
                    obj = binder.convertIfNecessary(value.toString(), info.getField().getType());
            }
        }
        if(obj != null){
            try {
                info.getField().setAccessible(true);
                info.getField().set(object, obj);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }
}
