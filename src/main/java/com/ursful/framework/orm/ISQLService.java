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
package com.ursful.framework.orm;

import com.ursful.framework.orm.exception.ORMException;
import com.ursful.framework.orm.support.ColumnClass;
import com.ursful.framework.orm.support.Table;
import com.ursful.framework.orm.support.TableColumn;

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

    <T> T queryObject(Class<T> clazz, String sql, Object ... params);

    <T> List<T> queryObjectList(Class<T> clazz, String sql, Object ... params);

    Map<String, Object> queryMap(String sql, Object ... params);

    List<Map<String, Object>> queryMapList(String sql, Object ... params);

    int queryCount(String sql, Object ... params);

    Object queryResult(String sql, Object ... params);

    Connection getConnection();

    Date getDatabaseDateTime();

    Double getDatabaseNanoTime();

    //自动提交，无回滚
    <S> List<S> batchSaves(List<S> ts, int batchCount);

    //无回滚
    <S> List<S> batchSaves(List<S> ts, int batchCount, boolean autoCommit);

    /**
     * 批量保存
     * @param ts
     * @param batchCount
     * @param autoCommit auto commit
     * @param rollback 当autoCommit=false 有效， rollback = false 不回滚， 为true 回滚
     * @param <S>
     * @return
     */
    <S> List<S> batchSaves(List<S> ts, int batchCount, boolean autoCommit, boolean rollback);

    //默认非自动提交 回滚
    <S> List<S> batchSaves(List<S> ts, boolean rollback);

    void createOrUpdate(Class<?> table) throws ORMException;

    boolean tableExists(String table);
    Table table(Class<?> clazz)  throws ORMException;
    Table table(String table);
    String getTableName(Class<?> clazz) throws ORMException;
    List<TableColumn> columns(Class<?> clazz) throws ORMException;

    List<Table> tables();
    List<Table> tables(String keyword);
    List<TableColumn> tableColumns(String tableName);
    List<ColumnClass> tableColumnsClass(String tableName);
}
