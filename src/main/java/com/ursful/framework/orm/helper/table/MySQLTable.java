package com.ursful.framework.orm.helper.table;

import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.support.DatabaseType;
import com.ursful.framework.orm.support.DynamicTable;
import org.springframework.core.annotation.AnnotationUtils;

import java.sql.Connection;

/**
 * 类名：MySQLTable
 * 创建者：huangyonghua
 * 日期：2019/3/9 12:46
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class MySQLTable  implements DynamicTable {
    @Override
    public DatabaseType databaseType() {
        return DatabaseType.MySQL;
    }

    @Override
    public void register(Class clazz, Connection connection) {
        RdTable rdTable = AnnotationUtils.findAnnotation(clazz, RdTable.class);
        if(rdTable == null){
            return;
        }
        String tableName = rdTable.name();

    }
}
