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
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
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

    private static Map<Class, DataSource> classDataSourceMap = new HashMap<Class, DataSource>();
    private static Map<String, DataSource> packageDataSourceMap = new HashMap<String, DataSource>();
    private static Map<Class, DataSource> serviceDataSourceMap = new HashMap<Class, DataSource>();

    public void registerService(Class clazz, DataSource dataSource){
        serviceDataSourceMap.put(clazz, dataSource);
    }

    public void register(Class clazz, DataSource dataSource){
        classDataSourceMap.put(clazz, dataSource);
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



    public DataSource getRawDataSource(){
        DataSource current = dataSource;
        if(dataSource instanceof DynamicDataSource){
            DynamicDataSource dynamicDataSource = (DynamicDataSource) dataSource;
            current = dynamicDataSource.currentDataSource();
        }
        return current;
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
    public DatabaseType getDatabaseType(Class clazz){
        DataSource raw = getDataSource(clazz);
        return getDatabaseType(raw);
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public QueryPage getQueryPage(){
        return getQueryPage(null);
    }
    public QueryPage getQueryPage(Class clazz){
        DatabaseType databaseType =  getDatabaseType(clazz);
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
        return getDynamicTable(null);
    }

    public DynamicTable getDynamicTable(Class clazz){
        DataSource raw = getDataSource(clazz);
        DatabaseType databaseType =  getDatabaseType(raw);
        String dbName = "null";
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
                classDataSourceMap.put(clazz, ds);
                return ds;
            }
        }
        return null;
    }

    public void clearPatternDataSource(){
        classDataSourceMap.clear();
        packageDataSourceMap.clear();
    }

    public DataSource getDataSource(Class clazz) {
        DataSource source = null;
        if(classDataSourceMap.containsKey(clazz)){
            source = classDataSourceMap.get(clazz);
        }else {
            source = getPackageDataSource(clazz);
            if(source == null) {
                source = getRawDataSource();
            }
        }
        return source;
    }

    public Connection getConnection(Class clazz, Class serviceClass){
        DataSource source = serviceDataSourceMap.get(serviceClass);
        if(source == null) {
            source = getDataSource(clazz);
        }
        Connection connection = DataSourceUtils.getConnection(source);
        DatabaseTypeHolder.set(getDatabaseType(source, connection));
        return connection;
    }


    public DatabaseType getDatabaseType(DataSource source, Connection connection){
        DatabaseType type = databaseTypeMap.get(source);
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
            databaseTypeMap.put(source, databaseType);
            return databaseType;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return DatabaseType.NONE;
    }


    public synchronized void close(Class clazz, ResultSet rs, Statement stmt, Connection conn){
        DataSource source = getDataSource(clazz);
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

    public synchronized void close(Class clazz, Statement stmt, Connection conn){
        close(clazz, null, stmt, conn);
    }

    public synchronized void close(Class clazz, Connection conn){
        close(clazz, null, null, conn);
    }

}
