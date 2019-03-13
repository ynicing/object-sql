package com.ursful.framework.orm.support;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.helper.SQLHelper;

import java.sql.Connection;

/**
 * Created by Administrator on 2018/7/5.
 */
public interface DynamicTable {
    DatabaseType databaseType();
    void register(Class clazz, Connection connection);
}
