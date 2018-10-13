package com.ursful.framework.orm;

import com.ursful.framework.orm.ISQLService;
import com.ursful.framework.orm.helper.SQLHelperCreator;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.DatabaseType;
import com.ursful.framework.orm.support.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.sql.Date;
import java.util.*;

public class SQLServiceImpl implements ISQLService{

    @Autowired(required = false)
    protected DataSourceManager dataSourceManager;

    public DataSourceManager getDataSourceManager() {
        return dataSourceManager;
    }

    @Override
    public String currentDatabaseType() {
        return dataSourceManager.getDatabaseType().name();
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
            close(null, ps, conn);
        }
    }

    private void setParams(PreparedStatement ps, Object[] params, Connection connection) throws SQLException {
        if(params != null && params.length > 0){
            List<Pair> pairList = new ArrayList<Pair>();
            for(Object object : params){
                pairList.add(new Pair(object));
            }
            SQLHelperCreator.setParameter(ps, pairList, dataSourceManager.getDatabaseType(), connection);
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
            close(rs, ps, conn);

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
            close(rs, ps, conn);
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
            close(rs, ps, conn);
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
            close(rs, ps, conn);
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
            close(rs, ps, conn);
        }
        return temp;
    }

    /*
    @Override
    public Map<String, Object> queryMap(String hql, Map<String, Object> params) {
        HQLParser parser = new HQLParser(hql, params);
        int size = parser.getParameters().size();
        return queryMap(parser.getSql(), parser.getParameters().toArray(new Object[size]));
    }

    @Override
    public List<Map<String, Object>> queryMapList(String hql, Map<String, Object> params) {
        HQLParser parser = new HQLParser(hql, params);
        int size = parser.getParameters().size();
        return queryMapList(parser.getSql(), parser.getParameters().toArray(new Object[size]));
    }

    @Override
    public int queryCount(String hql, Map<String, Object> params) {
        HQLParser parser = new HQLParser(hql, params);
        int size = parser.getParameters().size();
        return queryCount(parser.getSql(), parser.getParameters().toArray(new Object[size]));
    }

    @Override
    public Object queryResult(String hql, Map<String, Object> params) {
        HQLParser parser = new HQLParser(hql, params);
        int size = parser.getParameters().size();
        return queryResult(parser.getSql(), parser.getParameters().toArray(new Object[size]));
    }
    */

    public Connection getConnection(){
        DataSource source = dataSourceManager.getDataSource();
        if(source instanceof DynamicDataSource){
            DynamicDataSource dynamicDataSource = (DynamicDataSource)source;
            if(DynamicDataSource.getDataSource() != null){
                source = dynamicDataSource.currentDataSource();
            }
        }
        Connection conn = DataSourceUtils.getConnection(source);
        return conn;
    }

    protected void close(ResultSet rs, Statement statement, Connection connection){
        DataSource source = dataSourceManager.getDataSource();
        if(source instanceof DynamicDataSource){
            DynamicDataSource dynamicDataSource = (DynamicDataSource)source;
            if(DynamicDataSource.getDataSource() != null){
                source = dynamicDataSource.currentDataSource();
            }
        }
        dataSourceManager.close(rs, statement, connection, source);
    }


    public static void main(String[] args) {//:xx)><= *|&!^+-x/
        String sql = "select * from a where id=:xx and ";
    }



}
