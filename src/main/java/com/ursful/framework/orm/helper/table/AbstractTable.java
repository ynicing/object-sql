package com.ursful.framework.orm.helper.table;

import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.support.DynamicTable;
import org.springframework.core.annotation.AnnotationUtils;

import java.sql.Connection;

/**
 * 类名：AbstractTable
 * 创建者：huangyonghua
 * 日期：2019/3/9 12:55
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public abstract class AbstractTable implements DynamicTable{
    protected String getTableName(Class clazz){
        RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
        if(rdTable == null){
            return null;
        }
        return rdTable.name();
    }

    //((ExtAtomikosDataSourceBean)dataSource).xaProperties = URL
    //((DruidDataSource) dataSource).jdbcUrl
    //
    protected boolean tableExists(String tableName, Connection connection){
        //query table info.

//        case MySQL:
//        sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+db+"'";
//        break;
//        case Oracle:
//        sql = "select table_name from user_tables";
//        break;

//        String sql = "select column_name, nullable from USER_TAB_COLUMNS WHERE TABLE_NAME='" + tableName + "'";
//        if(dt == DatabaseType.MySQL){
//            sql = "select column_name, is_nullable from information_schema.columns where table_schema='" + dbName
//                    + "' and table_name='"+tableName+"'";
//        }

//        String sql = "select column_name, comments from USER_COL_COMMENTS WHERE TABLE_NAME='" + tableName + "'";
//        if(dt == DatabaseType.MySQL){
//            sql = "select column_name, column_comment from information_schema.columns where table_schema='" + dbName
//                    + "' and table_name='"+tableName+"'";
//        }

//        String sql = "select comments from USER_TAB_COMMENTS WHERE TABLE_NAME='" + tableName + "'";
//        if(dt == DatabaseType.MySQL){
//            sql = "SELECT table_comment FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+dbName+"' AND TABLE_NAME='" + tableName + "'";
//        }

//        case MySQL:
//        //String scheme = getMySQLScheme();
//        sql = "select column_key from information_schema.columns where table_schema='" + dbName
//                + "' and table_name='"+tableName+"' and column_name='"+ columnName +"'";
//        break;
//        case Oracle:
//        sql = "select au.constraint_type from user_cons_columns cu, user_constraints au where " +
//                " cu.constraint_name = au.constraint_name and au.constraint_type = 'P' " +
//                " and cu.table_name='"+tableName+"' and cu.column_name='"+columnName+"'";

//        sql = "select is_nullable from information_schema.columns where table_schema='" + dbName
//                + "' and table_name='"+tableName+"' and column_name='"+ columnName +"'";
//        break;
//        case Oracle:
//        sql = "select au.constraint_type from user_cons_columns cu, user_constraints au where " +
//                " cu.constraint_name = au.constraint_name and au.constraint_type = 'P' " +
//                " and cu.table_name='"+tableName+"' and cu.column_name='"+columnName+"'";
//
//        sql = "select nullable from user_tab_cols where table_name='" + tableName +
//                "' and column_name='"+ columnName + "'";

        return false;
    }
}
