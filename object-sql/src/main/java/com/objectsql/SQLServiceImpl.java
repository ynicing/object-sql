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
package com.objectsql;

import com.objectsql.annotation.RdId;
import com.objectsql.exception.ORMBatchException;
import com.objectsql.exception.ORMException;
import com.objectsql.exception.ORMSQLException;
import com.objectsql.handler.IResultSetHandler;
import com.objectsql.handler.ResultSetFunction;
import com.objectsql.support.*;
import com.objectsql.annotation.RdTable;
import com.objectsql.handler.DefaultResultSetHandler;
import com.objectsql.helper.SQLHelper;
import com.objectsql.helper.SQLHelperCreator;
import com.objectsql.utils.ORMUtils;
import org.springframework.util.StringUtils;

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
import java.time.ZoneOffset;
import java.util.*;
import java.time.LocalDateTime;

public class SQLServiceImpl implements ISQLService{

    public ObjectSQLManager objectSQLManager;

    public SQLServiceImpl(){

    }

    public SQLServiceImpl(ObjectSQLManager objectSQLManager){
        this.objectSQLManager = objectSQLManager;
    }

    public SQLServiceImpl(DataSource dataSource){
        this.objectSQLManager = new ObjectSQLManager(dataSource);
    }

    protected Class<?> thisClass;
    protected Class<?> serviceClass;
    protected IResultSetHandler resultSetHandler = new DefaultResultSetHandler();

    public IResultSetHandler getResultSetHandler() {
        return resultSetHandler;
    }

    public void setResultSetHandler(IResultSetHandler resultSetHandler) {
        this.resultSetHandler = resultSetHandler;
    }

    public ObjectSQLManager getObjectSQLManager() {
        return objectSQLManager;
    }

    public Options getOptions(){
        if(objectSQLManager == null){
            return null;
        }
        return objectSQLManager.getOptions(thisClass, serviceClass);
    }

    @Override
    public String currentDatabaseName() {
        if(objectSQLManager == null){
            return null;
        }
        return objectSQLManager.getProductName(thisClass, serviceClass);
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
        if(objectSQLManager == null){
            return null;
        }
        return objectSQLManager.getDataSource(thisClass, serviceClass);
    }

    @Override
    public void changeDataSource(String alias) {
        DynamicDataSource.setDataSource(alias);
    }

    public void setObjectSQLManager(ObjectSQLManager objectSQLManager) {
        this.objectSQLManager = objectSQLManager;
    }

    @Override
    public boolean execute(String sql, Object... params) {
        PreparedStatement ps = null;
        Connection conn = null;
        try {
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "execute", sql, params);
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            return ps.execute();
        } catch (SQLException e) {
            throw new ORMSQLException(e, "execute").put("sql", sql).put("params", params);
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
        return executeBatch(sql, false, true, params);
    }

    @Override
    public void query(String sql, Object[] params, ResultSetFunction function) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        try {
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "query", sql, params, function);
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
                function.process(rs);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "query").put("sql", sql).put("params", params).put("function", function);
        } finally{
            closeConnection(rs, ps, conn);
        }
    }

    @Override
    public boolean executeBatch(String sql, boolean autoCommit, boolean rollback, Object[]... params) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        boolean result = false;
        try {
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "executeBatch", sql, autoCommit, rollback, params);
            if(params != null){
                conn = getConnection();
                ps = conn.prepareStatement(sql);
                if(!autoCommit) {
                    conn.setAutoCommit(false);
                }
                for(int i = 0; i < params.length; i++) {
                    setParams(ps, params[i], conn);
                    ps.addBatch();
                }
                ps.executeBatch();
                ps.clearBatch();
                if (!autoCommit) {
                    conn.commit();
                }
                result = true;
            }else{
                return false;
            }
        } catch (SQLException e) {
            if (!autoCommit && rollback) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            }
            throw new ORMBatchException(e, params.length, -1);
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
    public <T> T queryObject(Class<T> clazz, String sql, Object... params) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        T temp = null;
        Connection conn = null;
        try {
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "queryObject", clazz, sql, params);
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
            throw new ORMSQLException(e, "query").put("class", clazz).put("sql", sql).put("params", params);
        } catch (IllegalAccessException e) {
            throw new ORMSQLException(e, "query").put("class", clazz).put("sql", sql).put("params", params);
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
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "queryObjectList", clazz, sql, params);

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
            throw new ORMSQLException(e, "queryObjectList").put("class", clazz).put("sql", sql).put("params", params);
        } catch (IllegalAccessException e) {
            throw new ORMSQLException(e, "queryObjectList").put("class", clazz).put("sql", sql).put("params", params);
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
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "queryMap", sql, params);
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            if(rs.next()){
                ResultSetMetaData metaMap = rs.getMetaData();
                tempMap = new HashMap<String, Object>();
                for(int i = 1; i <= metaMap.getColumnCount(); i++){
                    Object obj = rs.getObject(i);
                    KV kv = resultSetHandler.parseMap(metaMap, i, obj, rs);
                    if(kv != null) {
                        tempMap.put(kv.getKey(), kv.getValue());
                    }
                }
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "queryMap").put("sql", sql).put("params", params);
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
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "queryMapList", sql, params);
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            while(rs.next()){
                ResultSetMetaData metaMap = rs.getMetaData();
                Map<String, Object> tempMap = new HashMap<String, Object>();
                for(int i = 1; i <= metaMap.getColumnCount(); i++){
                    Object obj = rs.getObject(i);
                    KV kv = resultSetHandler.parseMap(metaMap, i, obj, rs);
                    if(kv != null) {
                        tempMap.put(kv.getKey(), kv.getValue());
                    }
                }
                temp.add(tempMap);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "queryMapList").put("sql", sql).put("params", params);
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
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "queryCount", sql, params);
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            if(rs.next()){
                temp = rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "queryCount").put("sql", sql).put("params", params);
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
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "queryResult", sql, params);
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            setParams(ps, params, conn);
            rs = ps.executeQuery();
            if(rs.next()){
                temp = rs.getObject(1);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "queryResult").put("sql", sql).put("params", params);
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public void closeConnection(ResultSet rs, Statement stmt, Connection conn){
        if(objectSQLManager == null){
            return;
        }
        objectSQLManager.close(thisClass, serviceClass, rs, stmt, conn);
    }

    @Override
    public Connection getConnection() {
        if(objectSQLManager == null){
            return null;
        }
        return objectSQLManager.getConnection(thisClass, serviceClass);
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
        if(objectSQLManager == null){
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
            Options options = objectSQLManager.getOptions(type);
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
                    return timestamp.getTime() + timestamp.getNanos() / 1000000000.0;
                }else if(object instanceof LocalDateTime) {
                    LocalDateTime ldt = (LocalDateTime) object;
                    int hour = TimeZone.getDefault().getRawOffset()/1000/3600;
                    return ldt.toInstant(ZoneOffset.ofHours(hour)).toEpochMilli() * 1.0;
                }else{
                    Timestamp timestamp = getOracleTimestamp(object, conn);
                    ORMUtils.whenEmpty(timestamp, "Error get timestamp from oracle.");
                    return timestamp.getTime() + timestamp.getNanos()/1000000000.0;
                }
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "getDatabaseNanoTime");
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
//            DynamicTable table = objectSQLManager.getDynamicTable(clazz, serviceClass);
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
            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "getOracleTimestamp exception:" + e.getMessage(), value);
            return null;
        }
    }

    private Connection getRealConnection(Connection connection){
        Connection conn = null;
        ObjectSQLManager sqlManager = getObjectSQLManager();
        if(sqlManager == null){
            return null;
        }
        List<IRealConnection> realConnections = sqlManager.getRealConnection();
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
    @Override
    public  <S> List<S> batchInserts(List<S> ts, int batchCount) {//与事务一起
        return batchInserts(ts, batchCount, true, false);
    }
    @Override
    public  <S> List<S> batchInserts(List<S> ts, int batchCount, boolean autoCommit) {
        return batchInserts(ts, batchCount, autoCommit, false);
    }
    @Override
    public <S> List<S> batchInserts(List<S> ts, boolean rollback) {
        return batchInserts(ts, ts.size(), rollback?false:true, rollback);
    }
    @Override
    public  <S> List<S> batchInserts(List<S> ts, int batchCount, boolean autoCommit, boolean rollback) {

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

            List<SQLHelper> helpers = SQLHelperCreator.inserts(ts, getOptions());

            if(helpers.isEmpty()){
                return new ArrayList<S>();
            }

            SQLHelper helper = helpers.get(0);

            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "batchInserts", helper, ts, batchCount, autoCommit, rollback);

            RdId rdId = null;

            if(helper.getIdValue() == null && helper.getIdField() != null) {
                rdId = helper.getIdField().getAnnotation(RdId.class);
            }

            conn = getConnection();

            if(!autoCommit) {
                conn.setAutoCommit(false);
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
                        int j = i;
                        while(seqRs.next()) {
                            Object key = seqRs.getObject(1);
                            S s = ts.get(j);
                            helper.setId(s, key);
                            j++;
                        }
                        seqRs.close();
                    }
                    ps.clearBatch();
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
            List<SQLHelper> helpers = SQLHelperCreator.updates(ts, columns == null ? null : Arrays.asList(columns));

            if(helpers.isEmpty()){
                return result;
            }
            SQLHelper helper = helpers.get(0);

            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "batchUpdates", helper);

            conn = getConnection();

            if(!autoCommit) {
                conn.setAutoCommit(false);
            }

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
                    ps.clearBatch();
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
        return batchUpdates(ts, columns, ts.size(), rollback?false:true, rollback);
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
        return batchUpdates(ts, null, ts.size(), rollback?false:true, rollback);
    }

    @Override
    public boolean batchUpdates(List ts, String[] columns, int batchCount) {
        return batchUpdates(ts, columns, batchCount, true, false);
    }

    @Override
    public boolean batchUpdates(List ts, String[] columns, int batchCount, boolean autoCommit) {
        return batchUpdates(ts, columns, batchCount, autoCommit, false);
    }

    //((JdbcConnection) connection).getURL() org.h2.jdbc.JdbcConnection
    //((JDBC4Connection) connection).getURL() com.mysql.jdbc.JDBC4Connection
    //((T4CConnection) connection).getURL() oracle.jdbc.driver.T4CConnection
    //connection.getCatalog() connection.getSchema() com.microsoft.sqlserver.jdbc.SQLServerConnection

    @Override
    public void createOrUpdate(Class<?> table)   throws ORMException{
        if(table == null){
            throw new ORMException("Table is null.");
        }
        RdTable rdTable = ORMUtils.getRdTable(table);
        if(rdTable == null){
            throw new ORMException(String.format("Class[%s] Table annotation(RdTable) not found.", table.getName()));
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMException(String.format("Create Or update Not Support. [%s]", table.getName()));
        }
        Connection temp = null;
        try {

            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "createOrUpdate", table);

            temp = getConnection();
            Connection connection = getRealConnection(temp);
            Table queryTable = options.table(connection, rdTable);
            List<ColumnInfo> columnInfoList = ORMUtils.getColumnInfo(table);
            if (queryTable == null && columnInfoList.isEmpty() && !rdTable.dropped()) {
                throw new ORMException(String.format("Class[%s] Table[%s], column(RdColumn) is empty.", table.getName(), rdTable.name()));
            }
            List<TableColumn> columns = null;
            if (queryTable != null && !rdTable.dropped()) {//judge Columns
                columns = options.columns(connection, rdTable);
            }
            List<String> sqls = options.createOrUpdateSqls(connection, rdTable, columnInfoList, queryTable != null, columns);
            if (sqls != null && !sqls.isEmpty()) {
                for (String sql : sqls) {
                    ORMUtils.handleDebugInfo(SQLServiceImpl.class, "createOrUpdate", sql, sqls, table);
                    PreparedStatement ps = null;
                    try {
                        ps = temp.prepareStatement(sql);
                        ps.execute();
                    } catch (SQLException e) {
                        throw new ORMException(e);
                    } finally {
                        closeConnection(null, ps, null);
                    }
                }
            }
        }catch (Exception e){
            throw new ORMSQLException(e, "createOrUpdate," + table.getName()).put("class", table);
        }finally{
            closeConnection(null, null, temp);
        }

    }

    @Override
    public void drop(Table table)   throws ORMException{
        if(table == null){
            throw new ORMException("Table is null.");
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMException(String.format("Drop table Not Support. [%s]", table.getName()));
        }
        Connection temp = null;
        try{

            ORMUtils.handleDebugInfo(SQLServiceImpl.class, "drop", table);

            temp = getConnection();
            Connection connection = getRealConnection(temp);
            Table queryTable  = options.table(connection, table);
            if(queryTable != null) {
                String sql = options.dropTable(table);

                ORMUtils.handleDebugInfo(SQLServiceImpl.class, "drop", table, sql);
                PreparedStatement ps = null;
                try {
                    ps = temp.prepareStatement(sql);
                    ps.execute();
                } catch (SQLException e) {
                    throw new ORMSQLException(e, "drop").put("table", table);
                } finally {
                    closeConnection(null, ps, null);
                }
            }else{
                ORMUtils.handleDebugInfo(SQLServiceImpl.class, "Table not exist : " + table.getName(), table);
            }
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public void createOrUpdate(Table table, List<TableColumn> tableColumns)   throws ORMException{
        if(table == null){
            throw new ORMException("Table is null.");
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Create Or update Table Not Support : " + table.getName()).put("table", table).put("tableColumns", tableColumns);
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            Table queryTable  = options.table(connection, table);

            List<TableColumn> columns = null;
            if(queryTable != null){//judge Columns
                columns = options.columns(connection, table.getName());
            }
            List<String> sqls = options.createOrUpdateSqls(connection, table, tableColumns, columns, queryTable != null);
            if(sqls != null && !sqls.isEmpty()){
                for(String sql : sqls){
                    ORMUtils.handleDebugInfo(SQLServiceImpl.class, "createOrUpdate", sql, sqls, table, tableColumns);

                    PreparedStatement ps = null;
                    try {
                        ps = temp.prepareStatement(sql);
                        ps.execute();
                    } catch (SQLException e) {
                        throw new ORMSQLException(e, "createOrUpdate").put("sql", sql).put("table", table).put("tableColumns", tableColumns);
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
        if(StringUtils.isEmpty(table)){
            throw new ORMException("Table is empty.");
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query Table Not Support : " + table).put("table", table);
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.tableExists(connection, table);
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "tableExists").put("table", table);
        }finally{
            closeConnection(null, null, temp);
        }
    }
    @Override
    public String getTableName(Class<?> clazz) throws ORMException{
        if(clazz == null){
            throw new ORMException("Class is null.");
        }
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        if(rdTable == null){
            throw new ORMException(String.format("Class[%s] Table annotation(RdTable) not found.", clazz.getName()));
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query Table Not Support : " + rdTable.name()).put("class", clazz);
        }
        return options.getTableName(rdTable);
    }

    @Override
    public Table table(Class<?> clazz) throws ORMException{
        if(clazz == null){
            throw new ORMException("Class is null.");
        }
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        if(rdTable == null){
            throw new ORMException(String.format("Class[%s] Table annotation(RdTable) not found.", clazz.getName()));
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query Table Not Support : " + rdTable.name()).put("class", clazz);
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.table(connection, rdTable);
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "table").put("class", clazz);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public Table table(String tableName){
        if(ORMUtils.isEmpty(tableName)){
            throw new ORMException("TableName is empty");
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query Table Not Support : " + tableName).put("tableName", tableName);
        }
        Connection temp = getConnection();
        try {
            Connection connection = getRealConnection(temp);
            return options.table(connection, new Table(tableName));
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "table").put("tableName", tableName);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<TableColumn> columns(Class<?> clazz) throws ORMException{
        if(clazz == null){
            throw new ORMException("Class is null");
        }
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        if(rdTable == null){
            throw new ORMException(String.format("Class[%s] Table annotation(RdTable) not found.", clazz.getName()));
        }
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query columns Not Support : " + clazz.getName()).put("class", clazz);
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
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "columns").put("class", clazz);
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
            throw new ORMSQLException("Query tables Not Support.").put("keyword", keyword);
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.tables(connection, keyword);
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "tables").put("keyword", keyword);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<TableColumn> tableColumns(String tableName) {
        Connection temp = getConnection();
        try{
           return tableColumns(temp, tableName);
        }finally{
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<TableColumn> tableColumns(Connection temp, String tableName) {
        List<TableColumn> columns = null;
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query Table Columns Not Support : " + tableName).put("connection", temp).put("tableName", tableName);
        }
        try {
            columns = options.columns(temp, tableName);
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "tableColumns").put("connection", temp).put("tableName", tableName);
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
        Connection temp = null;
        try {
            temp = getConnection();
            return tableColumnsClass(temp, tableName);
        }finally {
            closeConnection(null, null, temp);
        }
    }

    @Override
    public List<ColumnClass> tableColumnsClass(Connection temp, String tableName) {
        Options options = getOptions();
        if(options == null){
            throw new ORMSQLException("Query Table Columns Not Support : " + tableName).put("connection", temp).put("tableName", tableName);
        }
        Statement stmt = null;
        ResultSet rs = null;
        List<ColumnClass> columnClasses = new ArrayList<ColumnClass>();
        try {
            String sql = String.format("SELECT * FROM %s WHERE 1 <> 1", tableName);
            stmt = temp.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData data = rs.getMetaData();
            int count = data.getColumnCount();

            for (int i = 1; i <= count; i++) {
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
        }catch (ORMException e){
            throw e;
        }catch (Exception e){
            throw new ORMSQLException(e, "tableColumnsClass").put("connection", temp).put("tableName", tableName);
        }finally{
            closeConnection(rs, stmt, null);
        }
    }
}