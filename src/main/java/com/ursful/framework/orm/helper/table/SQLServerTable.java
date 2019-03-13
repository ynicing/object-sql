package com.ursful.framework.orm.helper.table;

import com.ursful.framework.orm.support.DatabaseType;
import com.ursful.framework.orm.support.DynamicTable;

import java.sql.Connection;

/**
 * 类名：MySQLTable
 * 创建者：huangyonghua
 * 日期：2019/3/9 12:46
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class SQLServerTable implements DynamicTable{
    @Override
    public DatabaseType databaseType() {
        return DatabaseType.SQLServer;
    }

    @Override
    public void register(Class clazz, Connection connection) {

    }
}
