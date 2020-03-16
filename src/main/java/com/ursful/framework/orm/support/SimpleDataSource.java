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
package com.ursful.framework.orm.support;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

public class SimpleDataSource implements DataSource {

    private String username;
    private String password;
    private String driver;
    private String url;

    public SimpleDataSource(){

    }

    public SimpleDataSource(String driver, String url, String username, String password){
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            Class.forName(this.driver);
            return DriverManager.getConnection(this.url, this.username, this.password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getConnection();
    }

    private PrintWriter printWriter;

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return printWriter;
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        this.printWriter = out;
    }

    private int timeout;

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        this.timeout = seconds;
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return this.timeout;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface == null) {
            return null;
        }

        if (iface.isInstance(this)) {
            return (T) this;
        }
        return null;
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface != null && iface.isInstance(this);
    }
}
