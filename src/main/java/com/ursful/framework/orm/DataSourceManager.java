package com.ursful.framework.orm;

import com.ursful.framework.orm.option.*;
import com.ursful.framework.orm.support.*;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

public class DataSourceManager {

    private static List<IRealConnection> connectionInterface = new ArrayList<IRealConnection>();

    public static List<IRealConnection> getRealConnection(){
        return connectionInterface;
    }

    public static void register(IRealConnection connection){
        if(!connectionInterface.contains(connection)){
            connectionInterface.add(connection);
        }
    }
    public static void deregister(IRealConnection connection){
        connectionInterface.remove(connection);
    }

    private static Map<DataSource, String> databaseTypeMap = new HashMap<DataSource, String>();
    private static List<Options> optionsList = Arrays.asList(
            new Options[]{new H2Options(),
                    new MySQLOptions(),
                    new SQLServerOptions(),
                    new OracleOptions(),
                    new PostgreSQLOptions()});

    private static Map<String, Options> optionsCache = new HashMap<String, Options>();

    private static Map<Class, DataSource> dataSourceMap = new HashMap<Class, DataSource>();
    private static Map<String, DataSource> packageDataSourceMap = new HashMap<String, DataSource>();
    private static Map<String, DataSource> classDataSourceMap = new HashMap<String, DataSource>();

    public void register(Class clazz, DataSource dataSource){
        classDataSourceMap.put(clazz.getName(), dataSource);
    }

    public void register(String pkg, DataSource dataSource){
        packageDataSourceMap.put(pkg, dataSource);
    }

    private DataSource dataSource;
    public DataSourceManager(){

    }

    public DataSourceManager(DataSource dataSource){
        this.dataSource = dataSource;
    }


    public DataSource getRawDataSource(DataSource current){
        DataSource raw = current;
        if(current instanceof DynamicDataSource){
            DynamicDataSource dynamicDataSource = (DynamicDataSource) current;
            raw = dynamicDataSource.currentDataSource();
        }
        return raw;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public Options getOptions(String type){
        return optionsCache.get(type);
    }

    public Options getOptions(Class clazz, Class serviceClass){
        String productName =  getProductName(clazz, serviceClass);
        Options options = optionsCache.get(productName);
        if(options == null){
            for(Options temp : optionsList){
                if(productName.contains(temp.keyword().toUpperCase(Locale.ROOT))){
                    options = temp;
                    break;
                }
            }
            optionsCache.put(productName, options);
        }
        return options;
    }

    public void registerOptions(Options options){
        if (!optionsList.contains(options)){
            optionsList.add(options);
        }
    }

    private DataSource getPackageDataSource(Class clazz){
        if(clazz == null || packageDataSourceMap.isEmpty()){
            return null;
        }
        String name = clazz.getName();
        for(String key : packageDataSourceMap.keySet()){
            if(name.startsWith(key)){
                DataSource ds = packageDataSourceMap.get(key);
                dataSourceMap.put(clazz, ds);
                return ds;
            }
        }
        return null;
    }

    public void clearPatternDataSource(){
        dataSourceMap.clear();
        classDataSourceMap.clear();
        packageDataSourceMap.clear();
    }

    private DataSource getPatternDataSource(Class clazz){
        if(clazz == null){
            return null;
        }

        String name = clazz.getName();
        DataSource ds = classDataSourceMap.get(name);
        if(ds != null){
            dataSourceMap.put(clazz, ds);
            return ds;
        }
        for(String key : packageDataSourceMap.keySet()){
            if(name.startsWith(key)){
                ds = packageDataSourceMap.get(key);
                dataSourceMap.put(clazz, ds);
                return ds;
            }
        }
        return null;
    }

    public DataSource getDataSource(Class clazz, Class serviceClass) {
        if(clazz == null && serviceClass == null){
            return dataSource;
        }
        DataSource source = null;
        if(serviceClass != null && dataSourceMap.containsKey(serviceClass)){
            source = dataSourceMap.get(serviceClass);
            return source;
        }
        if(clazz != null && dataSourceMap.containsKey(clazz)) {
            source = dataSourceMap.get(clazz);
            return source;
        }
        source = getPatternDataSource(serviceClass);
        if(source != null){
            return source;
        }
        source = getPatternDataSource(clazz);
        if(source != null){
            return source;
        }
        if(serviceClass != null) {
            dataSourceMap.put(serviceClass, dataSource);
        }
        if(clazz != null) {
            dataSourceMap.put(clazz, dataSource);
        }
        return dataSource;
    }


    public Connection getConnection(Class clazz, Class serviceClass){
        DataSource source = getDataSource(clazz, serviceClass);
        Connection connection = DataSourceUtils.getConnection(source);
        DatabaseTypeHolder.set(getProductName(source, connection));
        return connection;
    }

    public String getProductName(DataSource raw){
        String type = databaseTypeMap.get(raw);
        if(type != null){
            return type;
        }
        Connection connection = null;
        try {
            connection = DataSourceUtils.getConnection(raw);
            return getProductName(raw, connection);
        } finally {
            DataSourceUtils.releaseConnection(connection, raw);
        }
    }
    public String getProductName(Class clazz, Class serviceClass){
        DataSource temp = getDataSource(clazz, serviceClass);
        return getProductName(temp);
    }

    public String getProductName(DataSource source, Connection connection){
        DataSource raw = getRawDataSource(source);
        String type = databaseTypeMap.get(raw);
        if(type != null){
            return type;
        }
        try {
            String productName = connection.getMetaData().getDatabaseProductName().toUpperCase(Locale.ROOT);
            databaseTypeMap.put(raw, productName);
            return productName;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    public synchronized void close(Class clazz, Class serviceClass, ResultSet rs, Statement stmt, Connection conn){
        DataSource source = getDataSource(clazz, serviceClass);
        try {
            if(rs != null){
                rs.close();
            }
            if(stmt != null){
                stmt.close();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            DataSourceUtils.releaseConnection(conn, source);
            DatabaseTypeHolder.remove();
        }
    }


}
