package com.objectsql.handler;

import java.sql.ResultSet;

@FunctionalInterface
public interface ResultSetFunction {
    void process(ResultSet resultSet);
}
