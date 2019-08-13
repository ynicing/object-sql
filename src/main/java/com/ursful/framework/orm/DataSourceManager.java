package com.ursful.framework.orm;

import com.ursful.framework.orm.helper.table.H2Table;
import com.ursful.framework.orm.helper.table.MySQLTable;
import com.ursful.framework.orm.helper.table.OracleTable;
import com.ursful.framework.orm.helper.table.SQLServerTable;
import com.ursful.framework.orm.page.H2QueryPage;
import com.ursful.framework.orm.page.MySQLQueryPage;
import com.ursful.framework.orm.page.OracleQueryPage;
import com.ursful.framework.orm.page.SQLServerQueryPage;
import com.ursful.framework.orm.support.*;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private static Map<DataSource, DatabaseType> databaseTypeMap = new HashMap<DataSource, DatabaseType>();
    private static Map<DatabaseType, QueryPage> queryPageMap = new HashMap<DatabaseType, QueryPage>();
    private static Map<DatabaseType, DynamicTable> dynamicTableMap = new HashMap<DatabaseType, DynamicTable>();

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

    private List<QueryPage> queryPages;

    public void setQueryPages(List<QueryPage> queryPages) {
        this.queryPages = queryPages;
        for(QueryPage queryPage : queryPages){
            registerQueryPage(queryPage);
        }
    }

    private List<DynamicTable> dynamicTables;

    public void setDynamicTables(List<DynamicTable> dynamicTables) {
        this.dynamicTables = dynamicTables;
        for(DynamicTable dynamicTable : dynamicTables){
            registerDynamicTable(dynamicTable);
        }
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

    public QueryPage getQueryPage(){
        return getQueryPage(null, null);
    }
    public QueryPage getQueryPage(Class clazz, Class serviceClass){
        DatabaseType databaseType =  getDatabaseType(clazz, serviceClass);
        QueryPage queryPage = queryPageMap.get(databaseType);
        if(queryPage == null){
            switch (databaseType){
                case MySQL:
                    queryPage = new MySQLQueryPage();
                    break;
                case ORACLE:
                    queryPage = new OracleQueryPage();
                    break;
                case SQLServer:
                    queryPage = new SQLServerQueryPage();
                    break;
                case H2:
                    queryPage = new H2QueryPage();
                    break;
                default:
                    break;
            }
            queryPageMap.put(databaseType, queryPage);
        }
        return queryPage;
    }

    public DynamicTable getDynamicTable() {
        return getDynamicTable(null, null);
    }

    public DynamicTable getDynamicTable(Class clazz, Class serviceClass){
        DataSource raw = getDataSource(clazz, serviceClass);
        DatabaseType databaseType =  getDatabaseType(raw);
        //((ExtAtomikosDataSourceBean)dataSource).xaProperties = URL
        //((DruidDataSource) dataSource).jdbcUrl
        DynamicTable table = dynamicTableMap.get(databaseType);
        if(table == null){
            switch (databaseType){
                case MySQL:
                    table = new MySQLTable();
                    break;
                case ORACLE:
                    table = new OracleTable();
                    break;
                case SQLServer:
                    table = new SQLServerTable();
                    break;
                case H2:
                    table = new H2Table();
                    break;
                default:
                    break;
            }
            dynamicTableMap.put(databaseType, table);
        }
        return table;
    }

    public void registerDynamicTable(DynamicTable dynamicTable){
        dynamicTableMap.put(dynamicTable.databaseType(), dynamicTable);
    }


    public void registerQueryPage(QueryPage queryPage){
        queryPageMap.put(queryPage.databaseType(), queryPage);
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
        DatabaseTypeHolder.set(getDatabaseType(source, connection));
        return connection;
    }

    public DatabaseType getDatabaseType(DataSource raw){
        DatabaseType type = databaseTypeMap.get(raw);
        if(type != null){
            return type;
        }
        Connection connection = null;
        try {
            connection = DataSourceUtils.getConnection(raw);
            return getDatabaseType(raw, connection);
        } finally {
            DataSourceUtils.releaseConnection(connection, raw);
        }
    }
    public DatabaseType getDatabaseType(Class clazz, Class serviceClass){
        DataSource temp = getDataSource(clazz, serviceClass);
        return getDatabaseType(temp);
    }

    public DatabaseType getDatabaseType(DataSource source, Connection connection){
        DataSource raw = getRawDataSource(source);
        DatabaseType type = databaseTypeMap.get(raw);
        if(type != null){
            return type;
        }
        try {
            String productName = connection.getMetaData().getDatabaseProductName().toUpperCase();
            DatabaseType databaseType = null;
            if(productName.contains("MYSQL")){
                databaseType = DatabaseType.MySQL;
            }else if(productName.contains("ORACLE")){
                databaseType = DatabaseType.ORACLE;
            }else if(productName.contains("H2")){
                databaseType = DatabaseType.H2;
            }else if(productName.contains("SERVER")){
                databaseType = DatabaseType.SQLServer;
            }else{
                databaseType = DatabaseType.NONE;
                //throw new RuntimeException("Not support : " +productName);
            }
            databaseTypeMap.put(raw, databaseType);
            return databaseType;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return DatabaseType.NONE;
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
