package com.ursful.framework.orm;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * 类名：ISQLService
 * 创建者：huangyonghua
 * 日期：2018-02-22 16:56
 * 版权：厦门维途信息技术有限公司 Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public interface ISQLService {

    void setDataSourceManager(DataSourceManager dataSourceManager);

    //save update delete?
    boolean execute(String sql, Object ... params);

    boolean executeBatch(String sql, Object [] ... params);

    Map<String, Object> queryMap(String sql, Object ... params);

    List<Map<String, Object>> queryMapList(String sql, Object ... params);

    int queryCount(String sql, Object ... params);

    Object queryResult(String sql, Object ... params);

    Connection getConnection();
}
