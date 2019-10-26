package com.ursful.framework.orm;

import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.orm.helper.SQLHelperCreator;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.Assert;

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

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    protected Options getOptions(){
        return dataSourceManager.getOptions(thisClass, serviceClass);
    }

    @Override
    public String currentDatabaseName() {
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
//            ps.clearParameters();
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
                temp = SQLHelperCreator.newClass(clazz, rs);
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
                T tmp = SQLHelperCreator.newClass(clazz, rs);
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
                    if(obj != null) {
                        if (obj instanceof Timestamp) {
                            obj = new java.util.Date(((Timestamp) obj).getTime());
                        }
                        if((obj instanceof  Long) && metaMap.getPrecision(i) == 15) {
                            obj = new java.util.Date((Long)obj);
                        }
                        String key = QueryUtils.displayNameOrAsName(metaMap.getColumnLabel(i), metaMap.getColumnName(i));
                        tempMap.put(key, obj);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
                    if(obj != null) {
                        if (obj instanceof Timestamp) {
                            obj = new java.util.Date(((Timestamp) obj).getTime());
                        }
                        if((obj instanceof  Long) && metaMap.getPrecision(i) == 15) {
                            obj = new java.util.Date((Long)obj);
                        }
                        String key = QueryUtils.displayNameOrAsName(metaMap.getColumnLabel(i), metaMap.getColumnName(i));
                        tempMap.put(key, obj);
                    }
                }
                temp.add(tempMap);
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
            e.printStackTrace();
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
            e.printStackTrace();
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public void closeConnection(ResultSet rs, Statement stmt, Connection conn){
        dataSourceManager.close(thisClass, serviceClass, rs, stmt, conn);
    }

    @Override
    public Connection getConnection() {
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
                    Assert.notNull(timestamp, "Error get timestamp from oracle.");
                    return timestamp.getTime() + timestamp.getNanos()/1000000000.0;
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
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
            Assert.notNull(connection, "Oracle connection should not be nullable.");
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
        List<IRealConnection> realConnections = DataSourceManager.getRealConnection();
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
    public <S> List<S> batchSaves(List<S> ts, boolean rollback) {
        ResultSet rs = null;
        PreparedStatement ps = null;
        Connection conn = null;
        List<S> result = new ArrayList<S>();
        try {
            conn = getConnection();

            List<SQLHelper> helpers = SQLHelperCreator.inserts(ts);

            if(helpers.isEmpty()){
                return new ArrayList<S>();
            }
            SQLHelper helper = helpers.get(0);
            if(helper.getIdValue() == null && helper.getIdField() != null) {
                ps = conn.prepareStatement(helper.getSql(), Statement.RETURN_GENERATED_KEYS);
            }else{
                ps = conn.prepareStatement(helper.getSql());
            }
            conn.setAutoCommit(false);
            for(SQLHelper sqlHelper : helpers) {
                SQLHelperCreator.setParameter(getOptions(), ps, sqlHelper.getParameters(), conn);
                ps.addBatch();
            }
            ps.executeBatch();
//            ps.clearBatch();
//            ps.clearParameters();
            if(helper.getIdValue() == null && helper.getIdField() != null) {
                ResultSet seqRs = ps.getGeneratedKeys();
                int i = 0;
                int count = ts.size();
                while (seqRs.next() && i < count) {
                    Object key = seqRs.getObject(1);
                    S s = ts.get(i);
                    helper.setId(s, key);
                    i++;
                    result.add(s);
                }
                seqRs.close();
            }
        } catch (SQLException e) {
            if(rollback) {
                try {
                    conn.rollback();
                } catch (SQLException e1) {
                }
            }
            throw new RuntimeException(e);
        } finally{
            if(conn != null){
                try {
                    conn.setAutoCommit(true);
                } catch (SQLException e) {
                }
            }
            closeConnection(rs, ps, conn);

        }
        return result;
    }

    //((JdbcConnection) connection).getURL() org.h2.jdbc.JdbcConnection
    //((JDBC4Connection) connection).getURL() com.mysql.jdbc.JDBC4Connection
    //((T4CConnection) connection).getURL() oracle.jdbc.driver.T4CConnection
    //connection.getCatalog() connection.getSchema() com.microsoft.sqlserver.jdbc.SQLServerConnection

    @Override
    public void createOrUpdate(Class<?> table) {
        RdTable rdTable = ORMUtils.getRdTable(table);
        Assert.notNull(rdTable, "Error Entity, it must contain RdTable");
        Options options = getOptions();
        if(options == null){
            logger.warn("Create Or update Not Support.");
            return;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            Table queryTable  = options.table(connection, rdTable.name());
            List<ColumnInfo> columnInfoList = ORMUtils.getColumnInfo(table);
            List<TableColumn> columns = null;
            if(queryTable != null  &&  !rdTable.dropped()){//judge Columns
                columns = options.columns(connection, rdTable.name());
            }
            List<String> sqls = options.manageTable(rdTable, columnInfoList, queryTable != null, columns);
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
        }catch (Exception e){
            e.printStackTrace();
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
            e.printStackTrace();
        }finally{
            closeConnection(null, null, temp);
        }
        return false;
    }

    @Override
    public Table table(Class<?> clazz) {
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        Assert.notNull(rdTable, "Error Entity, it must contain RdTable");
        Options options = getOptions();
        if(options == null){
            logger.warn("Query table Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            return options.table(connection, rdTable.name());
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            closeConnection(null, null, temp);
        }
        return null;
    }

    @Override
    public List<TableColumn> columns(Class<?> clazz) {
        RdTable rdTable = ORMUtils.getRdTable(clazz);
        Assert.notNull(rdTable, "Error Entity, it must contain RdTable");
        Options options = getOptions();
        if(options == null){
            logger.warn("Create Or update Not Support.");
            return null;
        }
        Connection temp = getConnection();
        try{
            Connection connection = getRealConnection(temp);
            Table queryTable  = options.table(connection, rdTable.name());
            List<TableColumn> columns = null;
            if(queryTable != null  &&  !rdTable.dropped()){//judge Columns
                columns = options.columns(connection, rdTable.name());
            }
            return columns;
        }catch (Exception e){
            e.printStackTrace();
        }finally{
            closeConnection(null, null, temp);
        }
        return null;
    }
}