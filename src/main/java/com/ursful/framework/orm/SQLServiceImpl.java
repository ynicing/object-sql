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

import com.ursful.framework.orm.annotation.RdId;
import com.ursful.framework.orm.exception.ORMBatchException;
import com.ursful.framework.orm.exception.ORMError;
import com.ursful.framework.orm.exception.ORMException;
import com.ursful.framework.orm.handler.IResultSetHandler;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.handler.DefaultResultSetHandler;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.helper.SQLHelperCreator;
import com.ursful.framework.orm.utils.ORMUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Timestamp;
import java.util.*;

public class SQLServiceImpl implements ISQLService{

    private Logger logger = Logger.getLogger(SQLServiceImpl.class);

    @Autowired(required = false)
    protected DataSourceManager dataSourceManager;

    protected Class<?> thisClass;
    protected Class<?> serviceClass;
    protected IResultSetHandler resultSetHandler = new DefaultResultSetHandler();

    public IResultSetHandler getResultSetHandler() {
        return resultSetHandler;
    }

    public void setResultSetHandler(IResultSetHandler resultSetHandler) {
        this.resultSetHandler = resultSetHandler;
    }

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    public Options getOptions(){
        if(dataSourceManager == null){
            return null;
        }
        return dataSourceManager.getOptions(thisClass, serviceClass);
    }

    @Override
    public String currentDatabaseName() {
        if(dataSourceManager == null){
            return null;
        }
        return dataSourceManager.getProductName(thisClass, serviceClass);
    }


    @Override
    public String currentDatabaseType() {
        Options options = getOptions();
        if(options != null) {
            return getOptions().databaseType();
        }
        return null;
    }

    @Override
    public DataSource getDataSource() {
        if(dataSourceManager == null){
            return null;
        }
        return dataSourceManager.getDataSource(thisClass, serviceClass);
    }

    @Override
    public void changeDataSource(String alias) {
        DynamicDataSource.setDataSource(alias);
    }

    public void setDataSourceManager(DataSourceManager dataSourceManager) {
        this.dataSourceManager = dataSourceManager;
    }

    @Override
    public boolean execute(String sql, Object... params) {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            return ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            closeConnection(null, ps, conn);
        }
    }

    private void setParams(PreparedStatement ps, Object[] params, Connection connection) throws SQLException {
        if(params != null && params.length > 0){
            List<Pair> pairList = new ArrayList<Pair>();
            for(Object object : params){
                pairList.add(new Pair(object));
            }
            SQLHelperCreator.setParameter(getOptions(), ps, pairList, connection);
        }
    }

    @Override
    public boolean executeBatch(String sql, Object[]... params) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            conn.setAutoCommit(false);
            if(params != null){
                for(int i = 0; i < params.length; i++) {
                    setParams(ps, params[i], conn);
                    ps.addBatch();
                }
            }
            ps.executeBatch();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException e1) {
            }
            return false;
        } finally{
            if(conn != null){
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeConnection(rs, ps, conn);

        }
        return true;
    }

    @Override
    public <T> T queryObject(Class<T> clazz, String sql, Object... params) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        T temp = null;
        Connection conn = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            if(params != null) {
                List<Pair> pairList = new ArrayList<Pair>();
                for(Object param : params ){
                    pairList.add(new Pair(param));
                }
                SQLHelperCreator.setParameter(getOptions(), ps, pairList, conn);
            }
            rs = ps.executeQuery();
            if (rs.next()) {
                temp = SQLHelperCreator.newClass(clazz, rs, resultSetHandler);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + sql, e);
            throw new RuntimeException("QUERY_SQL_ERROR, OBJECT: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + sql, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, OBJECT: " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    @Override
    public <T> List<T> queryObjectList(Class<T> clazz, String sql, Object... params) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();
        Connection conn = null;
        try {
            conn = getConnection();

            ps = conn.prepareStatement(sql);
            if(params != null) {
                List<Pair> pairList = new ArrayList<Pair>();
                for(Object param : params ){
                    pairList.add(new Pair(param));
                }
                SQLHelperCreator.setParameter(getOptions(), ps, pairList, conn);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(clazz, rs, resultSetHandler);
                temp.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + sql, e);
            throw new RuntimeException("QUERY_SQL_ERROR, OBJECT LIST: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + sql, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, OBJECT LIST: " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    @Override
    public Map<String, Object> queryMap(String sql, Object... params) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        Map<String, Object> tempMap = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            if(rs.next()){
                ResultSetMetaData metaMap = rs.getMetaData();
                tempMap = new HashMap<String, Object>();
                for(int i = 1; i <= metaMap.getColumnCount(); i++){
                    Object obj = rs.getObject(i);
                    KV kv = resultSetHandler.parse(metaMap, i, obj, rs);
                    if(kv != null) {
                        tempMap.put(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            closeConnection(rs, ps, conn);
        }
        return tempMap;
    }

    @Override
    public List<Map<String, Object>> queryMapList(String sql, Object... params) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        List<Map<String, Object>> temp = new ArrayList<Map<String, Object>>();
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            while(rs.next()){
                ResultSetMetaData metaMap = rs.getMetaData();
                Map<String, Object> tempMap = new HashMap<String, Object>();
                for(int i = 1; i <= metaMap.getColumnCount(); i++){
                    Object obj = rs.getObject(i);
                    KV kv = resultSetHandler.parse(metaMap, i, obj, rs);
                    if(kv != null) {
                        tempMap.put(kv.getKey(), kv.getValue());
                    }
                }
                temp.add(tempMap);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    @Override
    public int queryCount(String sql, Object... params) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        int temp = 0;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            if(rs.next()){
                temp = rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public Object queryResult(String sql, Object... params) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        Object temp = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            if(rs.next()){
                temp = rs.getObject(1);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public void closeConnection(ResultSet rs, Statement stmt, Connection conn){
        if(dataSourceManager == null){
            return;
        }
        dataSourceManager.close(thisClass, serviceClass, rs, stmt, conn);
    }

    @Override
    public Connection getConnection() {
        if(dataSourceManager == null){
            return null;
        }
        return dataSourceManager.getConnection(thisClass, serviceClass);
    }

    @Override
    public Date getDatabaseDateTime() {
        Double longTime = getDatabaseNanoTime();
        if(longTime != null){
            return new Date(longTime.longValue());
        }
        return null;
    }

    public Double getDatabaseNanoTime(){
        if(dataSourceManager == null){
            return null;
        }
        ResultSet rs = null;
        Statement ps = null;
        Connection conn = null;
        try {
            conn = getConnection();
            String type = DatabaseTypeHolder.get();
            if(type == null){
                return null;
            }
            Options options = dataSourceManager.getOptions(type);
            if(options == null){
                return null;
            }
            String sql = options.nanoTimeSQL();
            if(sql == null){
                return null;
            }
            ps = conn.createStatement();
            rs = ps.executeQuery(sql);
            if(rs.next()){
                Object object = rs.getObject(1);
                if(object instanceof Timestamp) {
                    //timestamp.getNanos()/1000000000.0
                    //402855227933142
                    //  1552100302023
                    Timestamp timestamp = (Timestamp) object;
                    return timestamp.getTime() + timestamp.getNanos()/1000000000.0;
                }else{
                    Timestamp timestamp = getOracleTimestamp(object, conn);
                    ORMUtils.whenEmpty(timestamp, "Error get timestamp from oracle.");
                    return timestamp.getTime() + timestamp.getNanos()/1000000000.0;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            closeConnection(rs, ps, conn);
        }
        return null;
    }

//    @Override
//    public void register(Class clazz, Class serviceClass) {
//        ResultSet rs = null;
//        PreparedStatement ps = null;
//        Connection conn = null;
//        try {
//            //获取dataSource
//            DynamicTable table = dataSourceManager.getDynamicTable(clazz, serviceClass);
//            conn = getConnection();
//            table.register(clazz, conn);
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally{
//            closeConnection(rs, ps, conn);
//        }
//    }

    private Timestamp getOracleTimestamp(Object value, Connection connection) {
        try {
            ORMUtils.whenEmpty(connection, "Oracle connection should not be nullable.");
            Connection conn = getRealConnection(connection);
            Class clz = value.getClass();
            Method m = clz.getMethod("timestampValue", Connection.class);
            //m = clz.getMethod("timeValue", null); 时间类型
            //m = clz.getMethod("dateValue", null); 日期类型
            return (Timestamp) m.invoke(value, conn);
        } catch (Exception e) {
            return null;
        }
    }

    private Connection getRealConnection(Connection connection){
        Connection conn = null;
        List<IRealConnection> realConnections = getDataSourceManager().getRealConnection();
        if(!realConnections.isEmpty()){
            for (IRealConnection realConnection : realConnections){
                Connection temp = realConnection.getConnection(connection);
                if(temp != null){
                    conn = temp;
                    break;
                }
            }
        }
        if(conn == null && connection.getClass().getName().endsWith("DruidPooledConnection")){
            Class clazz = null;
            try {
                clazz = connection.getClass().getClassLoader().loadClass("com.alibaba.druid.pool.DruidPooledConnection");
                Method method = clazz.getMethod("getConnection");
                conn = (Connection)method.invoke(connection);
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.getTargetException();
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        if(conn == null){
            conn = connection;
        }
        return conn;
    }

    public  <S> List<S> batchSaves(List<S> ts, int batchCount) {//与事务一起
        return batchSaves(ts, batchCount, true, false);
    }

    public  <S> List<S> batchSaves(List<S> ts, int batchCount, boolean autoCommit) {
        return batchSaves(ts, batchCount, autoCommit, false);
    }

    public  <S> List<S> batchSaves(List<S> ts, int batchCount, boolean autoCommit, boolean rollback) {
        //autoCommit true, rollback  无效
        //autoCommit false rollback true: 回滚， false: 不回滚
        if(ts == null || ts.isEmpty()){
            return ts;
        }

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        int index = 0;
        try {
            conn = getConnection();

            if(!autoCommit) {
                conn.setAutoCommit(false);
            }

            List<SQLHelper> helpers = SQLHelperCreator.inserts(ts, getOptions());

            if(helpers.isEmpty()){
                return new ArrayList<S>();
            }
            SQLHelper helper = helpers.get(0);
            RdId rdId = null;

            if(helper.getIdValue() == null && helper.getIdField() != null) {
                rdId = helper.getIdField().getAnnotation(RdId.class);
            }
            if(rdId != null && rdId.autoIncrement()) {
                ps = conn.prepareStatement(helper.getSql(), Statement.RETURN_GENERATED_KEYS);
            }else{
                ps = conn.prepareStatement(helper.getSql());
            }
            for (int i = 0; i < helpers.size(); i += batchCount){
                index = i;
                int lastIndex = Math.min(i + batchCount, helpers.size()) - 1;
                if(i <= lastIndex){
                    for(int j = i; j <= lastIndex; j++){
                        SQLHelper sqlHelper = helpers.get(j);
                        SQLHelperCreator.setParameter(getOptions(), ps, sqlHelper.getParameters(), conn);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                    if(rdId != null && rdId.autoIncrement()) {
                        ResultSet seqRs = ps.getGeneratedKeys();
                        for(int j = i; j <= lastIndex; j++){
                            Object key = seqRs.getObject(1);
                            S s = ts.get(j);
                            helper.setId(s, key);
                        }
                        seqRs.close();
                    }
                }
            }
            if(!autoCommit){
                conn.commit();
            }
        } catch (SQLException e) {
            if(!autoCommit && rollback) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            }
            throw new ORMBatchException(e, ts.size(), index);
        } finally{
            if(conn != null && !autoCommit){
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeConnection(rs, ps, conn);

        }
        return ts;
    }

    public boolean batchUpdates(List ts, String[] columns,  int batchCount, boolean autoCommit, boolean rollback) {
        //autoCommit true, rollback  无效
        //autoCommit false rollback true: 回滚， false: 不回滚
        boolean result = false;
        if(ts == null || ts.isEmpty()){
            return result;
        }

        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        int index = 0;
        try {
            conn = getConnection();

            if(!autoCommit) {
                conn.setAutoCommit(false);
            }
            List<SQLHelper> helpers = SQLHelperCreator.updates(ts, columns == null ? null : Arrays.asList(columns));

            if(helpers.isEmpty()){
                return result;
            }
            SQLHelper helper = helpers.get(0);

            ps = conn.prepareStatement(helper.getSql());

            for (int i = 0; i < helpers.size(); i += batchCount){
                index = i;
                int lastIndex = Math.min(i + batchCount, helpers.size()) - 1;
                if(i <= lastIndex){
                    for(int j = i; j <= lastIndex; j++){
                        SQLHelper sqlHelper = helpers.get(j);
                        SQLHelperCreator.setParameter(getOptions(), ps, sqlHelper.getParameters(), conn);
                        ps.addBatch();
                    }
                    ps.executeBatch();
                }
            }
            if(!autoCommit){
                conn.commit();
            }
            result = true;
        } catch (SQLException e) {
            if(!autoCommit && rollback) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            }
            throw new ORMBatchException(e, ts.size(), index);
        } finally{
            if(conn != null && !autoCommit){
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeConnection(rs, ps, conn);

        }
        return result;
    }

    @Override
    public boolean batchUpdates(List ts, String[] columns, boolean rollback) {
        return batchUpdates(ts, columns, ts.size(), rollback, false);
    }

    @Override
    public boolean batchUpdates(List ts, int batchCount) {
        return batchUpdates(ts, null, ts.size(), true, false);
    }

    @Override
    public boolean batchUpdates(List ts, int batchCount, boolean autoCommit) {
        return batchUpdates(ts, null, batchCount, autoCommit, false);
    }

    @Override
    public boolean batchUpdates(List ts, int batchCount, boolean autoCommit, boolean rollback) {
        return batchUpdates(ts, null, batchCount, autoCommit, rollback);
    }

    @Override
    public boolean batchUpdates(List ts, boolean rollback) {
        return batchUpdates(ts, null, ts.size(), rollback, false);
    }

    @Override
    public boolean batchUpdates(List ts, String[] columns, int batchCount) {
        return batchUpdates(ts, columns, batchCount, true, false);
    }

    @Override
    public boolean batchUpdates(List ts, String[] columns, int batchCount, boolean autoCommit) {
        return batchUpdates(ts, columns, batchCount, autoCommit, false);
    }


    @Override
    public <S> List<S> batchSaves(List<S> ts, boolean rollback) {
        return batchSaves(ts, ts.size(), false, rollback);
    }



    //((JdbcConnection) connection).getURL() org.h2.jdbc.JdbcConnection
    //((JDBC4Connection) connection).getURL() com.mysql.jdbc.JDBC4Connection
    //((T4CConnection) connection).getURL() oracle.jdbc.driver.T4CConnection
    //connection.getCatalog() connection.getSchema() com.microsoft.sqlserver.jdbc.SQLServerConnection

    @Override
    public void createOrUpdate(Class<?> table)   throws ORMException{
        if(table == null){
            return;
        }
        RdTable rdTable = ORMUtils.getRdTable(table);
        if(rdTable == null){
            throw new ORMException(ORMError.CLASS_TABLE_ANNOTATION_NOT_FOUND, table.getName());
        }
        Options options = getOptions();
        if(options == null){
            logger.warn("Create Or update Not Support.");
            return;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            Table queryTable  = options.table(connection, rdTable);
            List<ColumnInfo> columnInfoList = ORMUtils.getColumnInfo(table);
            if(queryTable == null && columnInfoList.isEmpty() && !rdTable.dropped()){
                throw new ORMException(ORMError.CLASS_COLUMN_IS_EMPTY, table.getName(), rdTable.name());
            }
            List<TableColumn> columns = null;
            if(queryTable != null  &&  !rdTable.dropped()){//judge Columns
                columns = options.columns(connection, rdTable);
            }
            List<String> sqls = options.createOrUpdateSqls(connection, rdTable, columnInfoList, queryTable != null, columns);
            if(sqls != null && !sqls.isEmpty()){
                for(String sql : sqls){
                    PreparedStatement ps = null;
                    try {
                        ps = temp.prepareStatement(sql);
                        ps.execute();
                    } catch (SQLException e) {
                        logger.error("Execute SQL Error : " + sql);
                        throw new RuntimeException(e);
                    } finally{
                        closeConnection(null, ps, null);
                    }
                }
            }
        }finally{
            closeConnection(null, null, temp);
        }

    }

    @Override
    public boolean tableExists(String table) {
        Options options = getOptions();
        if(options == null){
            logger.warn("SQL Not Support.");
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.tableExists(connection, table);
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(null, null, temp);
        }
    }
    @Override
    public String getTableName(Class<?> clazz) throws ORMException{
        if(clazz == null){
            return null;
        }
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        if(rdTable == null){
            throw new ORMException(ORMError.CLASS_TABLE_ANNOTATION_NOT_FOUND, clazz.getName());
        }
        Options options = getOptions();
        if(options == null){
            logger.warn("Query table Not Support.");
            return null;
        }
        return options.getTableName(rdTable);
    }
    @Override
    public Table table(Class<?> clazz) throws ORMException{
        if(clazz == null){
            return null;
        }
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        if(rdTable == null){
            throw new ORMException(ORMError.CLASS_TABLE_ANNOTATION_NOT_FOUND, clazz.getName());
        }
        Options options = getOptions();
        if(options == null){
            logger.warn("Query table Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.table(connection, rdTable);
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public Table table(String tableName){
        Options options = getOptions();
        if(options == null){
            logger.warn("Query table Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.table(connection, tableName);
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<TableColumn> columns(Class<?> clazz) throws ORMException{
        if(clazz == null){
            return null;
        }
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        if(rdTable == null){
            throw new ORMException(ORMError.CLASS_TABLE_ANNOTATION_NOT_FOUND, clazz.getName());
        }
        Options options = getOptions();
        if(options == null){
            logger.warn("Create Or update Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            Table queryTable  = options.table(connection, rdTable);
            List<TableColumn> columns = null;
            if(queryTable != null  &&  !rdTable.dropped()){//judge Columns
                columns = options.columns(connection, rdTable);
            }
            return columns;
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<Table> tables() {
        return tables(null);
    }

    @Override
    public List<Table> tables(String keyword) {
        Options options = getOptions();
        if(options == null){
            logger.warn("Query Tables Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.tables(connection, keyword);
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<TableColumn> tableColumns(String tableName) {
        List<TableColumn> columns = null;
        Options options = getOptions();
        if(options == null){
            logger.warn("Query Table Columns Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            columns = options.columns(connection, tableName);
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(null, null, temp);
        }
        if(columns != null && !columns.isEmpty()){
            List<ColumnClass> columnClasses = tableColumnsClass(tableName);
            Map<String, String> columnClassMap = new HashMap<String, String>();
            for(ColumnClass columnClass : columnClasses){
                columnClassMap.put(columnClass.getColumn(), columnClass.getColumnClass());
                columnClassMap.put(columnClass.getColumn().toUpperCase(Locale.ROOT), columnClass.getColumnClass());
                columnClassMap.put(columnClass.getColumn().toLowerCase(Locale.ROOT), columnClass.getColumnClass());
            }
            for(TableColumn tableColumn : columns){
                tableColumn.setColumnClass(columnClassMap.get(tableColumn.getColumn()));
            }
        }
        return columns;
    }

    @Override
    public List<ColumnClass> tableColumnsClass(String tableName) {
        Options options = getOptions();
        if(options == null){
            logger.warn("Query Table Columns Not Support.");
            return null;
        }
        Connection temp = getConnection();
        Statement stmt = null;
        ResultSet rs = null;
        List<ColumnClass> columnClasses = new ArrayList<ColumnClass>();
        try{
            String sql = String.format("SELECT * FROM %s WHERE 1 <> 1", tableName);
            stmt = temp.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData data = rs.getMetaData();
            int count = data.getColumnCount();

            for(int i = 1; i <= count; i++){
                String className = data.getColumnClassName(i);
                String columnName = data.getColumnName(i);
                int precision = data.getPrecision(i);
                int scale = data.getScale(i);
                ColumnClass columnClass = new ColumnClass(tableName, columnName, className);
                columnClass.setPrecision(precision);
                columnClass.setScale(scale);
                columnClasses.add(columnClass);
            }
            return columnClasses;
        }catch (Exception e){
            throw new RuntimeException(e);
        }finally{
            closeConnection(rs, stmt, temp);
        }
    }
}