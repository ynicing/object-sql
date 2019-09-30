package com.ursful.framework.orm.option;

import com.ursful.framework.orm.support.ColumnType;
import com.ursful.framework.orm.support.DataType;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.sql.*;

public class PostgreSQLOptions extends MySQLOptions{
    @Override
    public String databaseType() {
        return "PostgreSQL";
    }

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i, Object obj, ColumnType columnType, DataType type) throws SQLException {
        if(type == DataType.STRING && obj != null){
            if(columnType == ColumnType.BLOB || columnType == ColumnType.CLOB) {
                try {
                    ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes("utf-8")));
                    return true;
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    @Override
    public String keyword() {
        return "PostgreSQL";
    }

    @Override
    public String nanoTimeSQL() {
        return "SELECT NOW()";
    }
}
