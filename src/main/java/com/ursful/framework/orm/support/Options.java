package com.ursful.framework.orm.support;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.helper.SQLHelper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface Options {
    String keyword();
    void setParameter(PreparedStatement ps, Connection connection,
                      String databaseType, int i, Object obj,
                      ColumnType columnType, DataType type) throws SQLException;
    String getColumnWithOperator(OperatorType operatorType, String name, String value);
    String getColumnWithOperatorAndFunction(String function, boolean inFunction,
                                OperatorType operatorType, String name, String value);
    String databaseType();
    String nanoTimeSQL();
    QueryInfo doQueryCount(IQuery query);
    QueryInfo doQuery(IQuery query, Pageable page);
    SQLHelper doQuery(Class<?> clazz, String[] names, Terms terms, MultiOrder multiOrder, Integer start, Integer size);
}
