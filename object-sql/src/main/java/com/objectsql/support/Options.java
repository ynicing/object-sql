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
package com.objectsql.support;

import com.objectsql.IQuery;
import com.objectsql.annotation.RdTable;
import com.objectsql.exception.ORMException;
import com.objectsql.helper.SQLHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface Options {
    String keyword();
    void setParameter(PreparedStatement ps, Connection connection,
                      String databaseType, int i, Pair pair) throws SQLException;
    String getColumnWithOperator(OperatorType operatorType, String name, String value);
    String getColumnWithOperatorAndFunction(String function, boolean inFunction,
                                OperatorType operatorType, String name, String value);
    String databaseType();
    String nanoTimeSQL();
    QueryInfo doQueryCount(IQuery query);
    QueryInfo doQuery(IQuery query);
    QueryInfo doQuery(IQuery query, Pageable page);
    SQLHelper doQuery(Class<?> clazz, String[] names, Condition condition, MultiOrder multiOrder, Integer start, Integer size);

    boolean tableExists(Connection connection, String table);
    boolean tableExists(Connection connection, RdTable table) throws ORMException;
    Table table(Connection connection, RdTable table) throws ORMException;
    Table table(Connection connection, Table table);
    List<TableColumn> columns(Connection connection, RdTable table) throws ORMException;

    SQLPair parseExpression(Object clazz, Expression expression);
    String getConditions(Class clazz, Expression [] expressions, List<Pair> values);
    String getConditions(Object queryOrClass, List<Condition> cds, List<Pair> values);
    String parseColumn(Column column);

    String getCaseSensitive(String name, int sensitive);
    String getTableName(RdTable table)  throws ORMException;

    List<Table> tables(Connection connection, String keyword);
    List<TableColumn> columns(Connection connection, String table);

    List<String> createOrUpdateSqls(Connection connection, RdTable table, List<ColumnInfo> columnInfoList, boolean tableExisted, List<TableColumn> tableColumns);
    List<String> createOrUpdateSqls(Connection connection, Table table, List<TableColumn> columns, List<TableColumn> tableColumns, boolean tableExisted);

    String getColumnType(TableColumn column);
    String getClassName(TableColumn column);
    String dropTable(Table table);

}
