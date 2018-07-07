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

import com.ursful.framework.orm.annotation.RdId;
import com.ursful.framework.orm.error.ORMErrorCode;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.core.exception.CommonException;
import org.springframework.validation.DataBinder;

import javax.sql.rowset.serial.SerialClob;
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
     * @return
     * @throws CommonException
     */
    public static SQLHelper delete(Object obj) throws CommonException{

        Class clazz = obj.getClass();


        List<Pair> parameters = new ArrayList<Pair>();

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("DELETE FROM ");

        sql.append(tableName + " ");

        List<String> ps = new ArrayList<String>();
        Pair primaryKey = null;
        for(Field field : clazz.getDeclaredFields()){
            RdId rid = (RdId)field.getAnnotation(RdId.class);
            RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);
            if(column != null && rid != null){
                Object fo = null;
                try {
                    field.setAccessible(true);
                    fo = field.get(obj);
                    if(fo != null){
                        primaryKey = new Pair(field.getName(), column.name(), field.getType().getSimpleName(), fo, null);
                        primaryKey.setColumnType(column.type());
                        break;
                    }
                } catch (IllegalArgumentException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                } catch (IllegalAccessException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ACCESS, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                }
            }
        }
        if(primaryKey != null && primaryKey.getValue() != null){
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.clear();
            parameters.add(primaryKey);
        }else{
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_DELETE_WITHOUT_ID, "Class(" + clazz.getName() +
                    "), value(" + obj + ")");
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
     * @return
     * @throws CommonException
     */
    public static SQLHelper delete(Class clazz, Object idObject) throws CommonException{

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("DELETE FROM ");

        sql.append(tableName + " ");

        List<Pair> parameters = new ArrayList<Pair>();
        List<String> ps = new ArrayList<String>();
        Pair primaryKey = null;


        for(Field field : clazz.getDeclaredFields()){
            RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);
            RdId id = (RdId) field.getAnnotation(RdId.class);
            if(column != null && id != null){
                try {
                    primaryKey = new Pair(field.getName(), column.name(), field.getType().getSimpleName(), idObject, null);
                    primaryKey.setColumnType(column.type());
                } catch (IllegalArgumentException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + idObject + ")");
                }
                break;
            }
        }
        if(primaryKey != null && primaryKey.getValue() != null){
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_DELETE_WITHOUT_ID, "Class(" + clazz.getName() +
                    "), value(" + idObject + ")");
        }

        SQLHelper helper = new SQLHelper();
        helper.setPair(primaryKey);
        helper.setSql(sql.toString());
        helper.setParameters(parameters);

        return helper;

    }

    public static SQLHelper deleteBy(Class clazz, Express[] expresses) throws CommonException{

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("DELETE FROM " + tableName + " WHERE ");
        List<Pair> pairs = new ArrayList<Pair>();
        for(Express express : expresses){
            SQLPair pair = QueryUtils.parseExpression(express.getExpression());
            sql.append(pair.getSql());
            pairs.addAll(pair.getPairs());
        }
        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(pairs);
        return helper;

    }

    /**
     * 更新 只能使用id，否则初学者不填id 全部更新了。
     * @param obj
     * @param updateNull
     * @return
     * @throws CommonException
     */
	public static SQLHelper update(Object obj, boolean updateNull) throws CommonException{

		Class clazz = obj.getClass();

		RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
		if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
		}
		String tableName = table.name();

		StringBuffer sql = new StringBuffer("UPDATE ");

		sql.append(tableName);
		sql.append(" SET ");

		List<Pair> parameters = new ArrayList<Pair>();
		List<String> ps = new ArrayList<String>();
		Pair primaryKey = null;
		for(Field field : clazz.getDeclaredFields()){
			RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);


			if(column != null){
                Object fo = null;
				try {
					field.setAccessible(true);
                    String type = field.getType().getSimpleName();
					fo = field.get(obj);
					if(fo != null || updateNull){
                        RdId id = (RdId) field.getAnnotation(RdId.class);
                        if(id != null){
                            if(fo == null){
                                throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_UPDATE_WITHOUT_ID, "Class(" + clazz.getName() +
                                        "), value(" + obj + ")");
                            }
							primaryKey = new Pair(field.getName(), column.name(), type, fo, null);
						}else{
                            if(fo == null && updateNull){
                                ps.add(column.name() + " = NULL ");
                            }else {
                                ps.add(column.name() + " = ? ");
                                Pair pair = new Pair(field.getName(), column.name(), type, fo, null);
                                pair.setColumnType(column.type());
//                                if (ls != null) {
//                                    if (Manager.getManager().getDatabaseType() == DatabaseType.MYSQL) {
//                                        pair.setLargeType(LargeType.LARGE_MYSQL_TEXT);
//                                    } else if (Manager.getManager().getDatabaseType() == DatabaseType.ORACLE) {
//                                        pair.setLargeType(LargeType.LARGE_ORACLE_CLOB);
//                                    } else if (Manager.getManager().getDatabaseType() == DatabaseType.SQLServer) {
//                                        pair.setLargeType(LargeType.LARGE_SQL_SERVER_NTEXT);
//                                    } else {
//                                        //parameters.add(new Pair("",type,  fo));
//                                    }
//                                }
                                parameters.add(pair);
                            }
						}
					}
                } catch (IllegalArgumentException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                } catch (IllegalAccessException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ACCESS, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                }
			}
		}
        if(primaryKey != null && primaryKey.getValue() != null) {
            sql.append(ps.toString().substring(1, ps.toString().length() - 1));
            sql.append(" WHERE ");
            sql.append(primaryKey.getColumn() + " = ? ");
            parameters.add(primaryKey);
        }else{
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_UPDATE_WITHOUT_ID, "Class(" + clazz.getName() +
                    "), value(" + obj + ")");
        }
		SQLHelper helper = new SQLHelper();
        helper.setPair(primaryKey);
		helper.setSql(sql.toString());
		helper.setParameters(parameters);

		//LogUtil.info("SQL:" + sql, SQLHelperCreator.class);

		return helper;

	}

	public static SQLHelper save(Object obj) throws CommonException{

		Class clazz = obj.getClass();

		RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
		if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
		}
		String tableName = table.name();

		StringBuffer sql = new StringBuffer("INSERT INTO ");

		sql.append(tableName);
		sql.append("(");

		List<Pair> parameters = new ArrayList<Pair>();
		List<String> ps = new ArrayList<String>();
		List<String> vs = new ArrayList<String>();

		SQLHelper helper = new SQLHelper();

		for(Field field : clazz.getDeclaredFields()){
			RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);
			RdId id = (RdId)field.getAnnotation(RdId.class);
			if(column != null){
                Object fo = null;
				try {
					field.setAccessible(true);
                    String type = field.getType().getSimpleName();
					fo = field.get(obj);
					if(fo != null || (id != null && "String".equals(type))){
                        if((fo == null || "".equals(fo.toString().trim())) && id != null){
                            fo = UUID.randomUUID().toString();
                            field.setAccessible(true);
                            field.set(obj, fo);
                        }
						ps.add(column.name());
						//parameters.add(fo);
						vs.add("?");
                        Pair pair = new Pair(field.getName(), column.name(), type, fo, null);
                        pair.setColumnType(column.type());
                        parameters.add(pair);
					}else{//值为空的时候，但是无id，需要自取了
                        if(id  != null){
                            helper.setIdField(field);
                        }
                    }
				} catch (IllegalArgumentException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
				} catch (IllegalAccessException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ACCESS, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
				}
			}
		}

        if(ps.size() == 0){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SAVE_WITHOUT_VALUE, "Class(" + clazz.getName() +
                    "), value(" + obj + ")");
        }
		sql.append(ORMUtils.join(ps, ","));
		sql.append(") VALUES (");
		sql.append(ORMUtils.join(vs, ","));
		sql.append(")");


		helper.setSql(sql.toString());
		helper.setParameters(parameters);

		return helper;

	}


    /**
     * 允许获取 id，匹配，唯一等值
     * 有id根据id获取，其他根据列 等值获取
     * @param obj
     * @return
     * @throws CommonException
     */
	public static SQLHelper get(Object obj) throws CommonException{

		Class clazz = obj.getClass();

		RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
		if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
		}
		String tableName = table.name();

		StringBuffer sql = new StringBuffer("SELECT * FROM ");
		sql.append(tableName);

		List<Pair> parameters = new ArrayList<Pair>();
		Map<String, DataType> types = new HashMap<String, DataType>();
        List<String> ps = new ArrayList<String>();
        Pair primaryKey = null;

		for(Field field : clazz.getDeclaredFields()){
			RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);
            if(column != null){
                Object fo = null;
                try {
                    field.setAccessible(true);
                    String type = field.getType().getSimpleName();
                    fo = field.get(obj);
                    if(fo != null){
                        RdId id = (RdId) field.getAnnotation(RdId.class);
                        if(id != null){
                            primaryKey = new Pair(field.getName(), column.name(), type, fo, null);
                            primaryKey.setColumnType(column.type());
                            break;
                        }else{
                            ps.add(column.name() + " = ? ");
                            Pair temp = new Pair(field.getName(), column.name(), type, fo, null);
                            temp.setColumnType(column.type());
                            parameters.add(temp);
                        }
                    }
                } catch (IllegalArgumentException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                } catch (IllegalAccessException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ACCESS, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                }
            }
		}

        if(primaryKey != null && primaryKey.getValue() != null) {
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

    public static SQLHelper query(Class<?> clazz, Express [] expresses) throws CommonException{

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("SELECT * FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(expresses != null) {
            String conditions = QueryUtils.getConditions(expresses, values);
            if (conditions != null && !"".equals(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;

    }



    public static SQLHelper queryCountExpress(Class<?> clazz, Express ... expresses) throws CommonException{

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(expresses != null) {
            String conditions = QueryUtils.getConditions(expresses, values);
            if (conditions != null && !"".equals(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;

    }

    public static SQLHelper queryCount(Class<?> clazz, Terms terms) throws CommonException{

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM " + tableName);
        List<Pair> values = new ArrayList<Pair>();
        if(terms != null) {
            String conditions = QueryUtils.getConditions(ORMUtils.newList(terms.getCondition()), values);
            if (conditions != null && !"".equals(conditions)) {
                sql.append(" WHERE " + conditions);
            }
        }


        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(values);

        return helper;

    }

    /**
     * 只允许ID
     * @param obj
     * @param clazz
     * @return
     * @throws CommonException
     */
    public static SQLHelper get(Object obj, Class clazz) throws CommonException{

        RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
        if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
        }
        String tableName = table.name();

        StringBuffer sql = new StringBuffer("SELECT * ");
        sql.append("FROM ");
        sql.append(tableName);
        sql.append(" WHERE ");
        List<Pair> parameters = new ArrayList<Pair>();
        Pair primaryKey = null;
        for(Field field : clazz.getDeclaredFields()){
            RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);
			RdId id = (RdId) field.getAnnotation(RdId.class);
            if(column != null && id != null && obj != null){
                   // DataType dt =  DataType.getDataType(field.getType().getSimpleName());
                    //types.put(field.getName(), dt);
                primaryKey = new Pair(field.getName(),column.name(), field.getType().getSimpleName(), obj, null);
                primaryKey.setColumnType(column.type());
                break;
            }
        }
        if(primaryKey == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_GET_WITHOUT_ID, "Class(" + clazz.getName() +
                    "), value(" + obj + ")");
        }

        sql.append(primaryKey.getColumn() + " = ? ");
        parameters.add(primaryKey);

        SQLHelper helper = new SQLHelper();
        helper.setSql(sql.toString());
        helper.setParameters(parameters);
        return helper;

    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static SQLHelper query(Object obj) throws CommonException{

		Class clazz = obj.getClass();

		RdTable table = (RdTable)clazz.getAnnotation(RdTable.class);
		if(table == null){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_NOT_FOUND, "Class(" + clazz.getName() + ")");
		}
		String tableName = table.name();

		StringBuffer sql = new StringBuffer("SELECT * FROM ");

		sql.append(tableName + " t ");
		sql.append(" WHERE 1 = 1 ");

		List<Pair> parameters = new ArrayList<Pair>();
		Map<String, DataType> types = new HashMap<String, DataType>();
		for(Field field : clazz.getDeclaredFields()){
			RdColumn column = (RdColumn) field.getAnnotation(RdColumn.class);
			if(column != null){
                Object fo = null;
				try {
					DataType dt =  DataType.getDataType(field.getType().getSimpleName());
					types.put(field.getName(), dt);
					field.setAccessible(true);
                    String type = field.getType().getSimpleName();
					fo = field.get(obj);
					if(fo != null){
						sql.append(" AND " + column.name() + " = ? ");
                        Pair pair = new Pair(field.getName(), column.name(), type, fo, null);
                        //查询不需要这个了。RdLargeString
                        parameters.add(pair);
					}
                } catch (IllegalArgumentException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                } catch (IllegalAccessException e) {
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_SET_PARAMETER_ILLEGAL_ACCESS, "Column(" + column.name() +
                            "), field(" + field.getName() + "), value(" + fo + ")");
                }
			}
		}

		SQLHelper helper = new SQLHelper();
		helper.setSql(sql.toString());
		helper.setParameters(parameters);
		return helper;
				
	}
	

	
	private static List<Field> getFieldList(Class<?> clazz){
		List<Field> list = new ArrayList<Field>();
		while(clazz != null){
			for(Field f : clazz.getDeclaredFields()){
				list.add(f);
			}
			clazz = clazz.getSuperclass();
		}
		return list;
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
//
//            case BOOLEAN:
//                Object tmp = rs.getObject(1);
//                if(tmp == null){
//                    t = (T)Boolean.valueOf(false);
//                }else {
//                    t = (T) Boolean.valueOf(
//                        "true".equalsIgnoreCase(tmp.toString())
//                        || "1".equalsIgnoreCase(tmp.toString())
//                        || "yes".equalsIgnoreCase(tmp.toString())
//                        || "on".equalsIgnoreCase(tmp.toString())
//                    );
//                }
//                break;
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
                        }else if((obj instanceof  Long) && metaMap.getPrecision(i) == 15) {
                            obj = new Date((Long)obj);
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
                for(int i = 1; i <= meta.getColumnCount(); i++){
                    String tmp = QueryUtils.displayNameOrAsName(meta.getColumnLabel(i), meta.getColumnName(i));
                    temp.put(tmp, rs.getObject(meta.getColumnLabel(i)));
                }
                List<Field> fields = getFieldList(clazz);
                for(Field field : fields){
                    if(temp.containsKey(field.getName())){
                        Object obj = getFieldObject(field, temp.get(field.getName()));
                        field.setAccessible(true);
                        field.set(t, obj);;
                    }
                }
                break;
            case INTEGER:
                t = (T)new Integer(rs.getInt(1));
        }

        return t;

	}


	public static <T> Object getFieldObject(Field field, Object object) throws IllegalAccessException, SQLException {
		Object obj = null;
        DataType type = DataType.getDataType(field.getType().getSimpleName());


        if(object != null){
            switch (type) {
                case BINARY:
                    if(object instanceof byte[]){
                        obj = object;
                    }else if(object instanceof Blob){
                        Blob blob = (Blob) object;
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
                        obj = object;
                    }
                    break;
                case STRING:
                    if(object instanceof Clob){
                        StringBuffer sb = new StringBuffer();
                        Clob clob = (Clob) object;
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
                    }else if(object instanceof Blob){
                        Blob blob = (Blob) object;
                        InputStream stream = blob.getBinaryStream();
                        try {
                            byte[] temp = new byte[(int)blob.length()];
                            stream.read(temp);
                            stream.close();
                            obj = new String(temp, "utf-8");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }else if(object instanceof byte[]){
                        try {
                            obj = new String((byte[])object, "utf-8");
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                    }else{
                        obj = object;
                    }
                    break;
                case DATE:
                    if(object instanceof Long) {
                        Long ts = (Long) object;
                        if (ts != null && ts.longValue() > 0) {
                            obj = new Date(ts);
                        }
                    }else if(object instanceof java.sql.Date){
                        java.sql.Date tmp = (java.sql.Date) object;
                        obj = new Date(tmp.getTime());
                    }else if(object instanceof java.sql.Timestamp){
                        Timestamp ts =  (Timestamp)object;
                        if(ts != null) {
                            obj = new Date(ts.getTime());
                        }
                    }else if(object instanceof BigDecimal){
                        BigDecimal ts =  (BigDecimal)object;
                        if(ts != null) {
                            obj = new Date(ts.longValue());
                        }
                    }else{
                        throw new RuntimeException("not support");
                    }
                    break;
                default:
                    if(object instanceof BigDecimal){
                        object = ((BigDecimal) object).toPlainString();
                    }else if(object instanceof Double || object instanceof Long || object instanceof Float){
                        object = new BigDecimal(object.toString()).toPlainString();
                    }
                    DataBinder binder = new DataBinder(field, field.getName());
                    obj = binder.convertIfNecessary(object.toString(), field.getType());
            }
        }

		return obj;
	}
	
	
	public static void setParameter(PreparedStatement ps, List<Pair> objects) throws SQLException{

		//ps.setObject(); 是否可以统一使用
		for(int i = 0; i < objects.size(); i++){
			Pair pair = objects.get(i);
			Object obj = pair.getValue();
			ColumnType columnType = pair.getColumnType();
			DataType type =  DataType.getDataType(pair.getType());
            switch (type) {
                case BINARY:
                    if(obj != null) {
                        if(obj instanceof byte[]) {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream((byte[]) obj));
                        }else{
                            try {
                                ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
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
                                ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
                            } catch (UnsupportedEncodingException e) {
                                e.printStackTrace();
                            }
                        }else{
                            ps.setBinaryStream(i + 1, null);
                        }
                    }else if(columnType == ColumnType.BLOB){
                        if(obj != null) {
                            try {
                                ps.setBlob(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
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
                            ps.setLong(i + 1, ((Date) obj).getTime());
                        }else if(columnType == ColumnType.DATETIME) {
                            Date date = (Date)obj;
                            ps.setDate(i + 1, new java.sql.Date(date.getTime()));
                        }else{//timestamp
                            ps.setTimestamp(i + 1, new Timestamp(((Date) obj).getTime()));
                        }
					}
					break;
				default:
					ps.setObject(i + 1, obj);
					break;
			}
		}
	}


    /*public static void setParameter(PreparedStatement ps, Object [] objects) throws SQLException{

        if(objects == null){
            return;
        }

        for(int i = 0; i < objects.length; i++){
            Object obj = objects[i];
            DataType type =  DataType.getDataType(obj.getClass().getSimpleName());
            switch (type) {
                case DATE:
                    ps.setTimestamp(i + 1, new Timestamp(((Date)obj).getTime()));
					break;
                default:
					ps.setObject(i + 1, obj);
                    break;
            }
        }
    }*/
	





}
