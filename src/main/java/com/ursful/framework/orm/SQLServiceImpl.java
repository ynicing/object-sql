package com.ursful.framework.orm;

import com.ursful.framework.orm.helper.SQLHelperCreator;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.DatabaseType;
import com.ursful.framework.orm.support.DatabaseTypeHolder;
import com.ursful.framework.orm.support.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.sql.DataSource;
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

    @Autowired(required = false)
    protected DataSourceManager dataSourceManager;

    protected Class<?> thisClass;

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    @Override
    public String currentDatabaseType() {
        DatabaseType type = dataSourceManager.getDatabaseType(thisClass);
        if(type != null){
            return type.name();
        }
        return null;
    }

    @Override
    public DataSource getDataSource() {
        return dataSourceManager.getDataSource(thisClass);
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
        int res = -1;
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
            SQLHelperCreator.setParameter(ps, pairList, connection);
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
        dataSourceManager.close(thisClass, rs, stmt, conn);
    }

    @Override
    public Connection getConnection() {
        return dataSourceManager.getConnection(thisClass);
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
            DatabaseType type = DatabaseTypeHolder.get();
            if(type == null){
                return null;
            }
            String sql = null;
            switch (type){
                case H2:
                case MySQL:
                    sql = "SELECT NOW(3)";
                    break;
                case SQLServer:
                    sql = "SELECT GETDATE()";
                    break;
                case ORACLE:
                    sql = "SELECT SYSTIMESTAMP FROM DUAL";
                    break;
            }
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

    private Timestamp getOracleTimestamp(Object value, Connection connection) {
        try {
            Class clz = value.getClass();
            Method m = clz.getMethod("timestampValue", Connection.class);
            //m = clz.getMethod("timeValue", null); 时间类型
            //m = clz.getMethod("dateValue", null); 日期类型
            return (Timestamp) m.invoke(value, connection);
        } catch (Exception e) {
            return null;
        }
    }
}