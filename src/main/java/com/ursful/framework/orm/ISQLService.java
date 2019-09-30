package com.ursful.framework.orm;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface ISQLService {

    String currentDatabaseName();

    String currentDatabaseType();

    DataSource getDataSource();

    DataSourceManager getDataSourceManager();

    void changeDataSource(String alias);

    void setDataSourceManager(DataSourceManager dataSourceManager);

    boolean execute(String sql, Object ... params);

    boolean executeBatch(String sql, Object [] ... params);

    Map<String, Object> queryMap(String sql, Object ... params);

    List<Map<String, Object>> queryMapList(String sql, Object ... params);

    int queryCount(String sql, Object ... params);

    Object queryResult(String sql, Object ... params);

    Connection getConnection();

    Date getDatabaseDateTime();

    Double getDatabaseNanoTime();

    <S> List<S> batchSaves(List<S> ts, boolean rollback);
}
