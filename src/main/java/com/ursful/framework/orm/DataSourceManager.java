package com.ursful.framework.orm;

import com.ursful.framework.core.datasource.DynamicDataSource;
import com.ursful.framework.orm.page.H2QueryPage;
import com.ursful.framework.orm.page.MySQLQueryPage;
import com.ursful.framework.orm.page.OracleQueryPage;
import com.ursful.framework.orm.page.SQLServerQueryPage;
import com.ursful.framework.orm.support.DatabaseType;
import com.ursful.framework.orm.support.QueryPage;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataSourceManager {

    private static Map<DataSource, DatabaseType> databaseTypeMap = new HashMap<DataSource, DatabaseType>();
    private static Map<DatabaseType, QueryPage> queryPageMap = new HashMap<DatabaseType, QueryPage>();

    private DataSource dataSource;
    public DataSourceManager(){}
    public DataSourceManager(DataSource dataSource){
        this.dataSource = dataSource;
    }

    private List<QueryPage> queryPageList;
    public void setQueryPageList(List<QueryPage> queryPageList){
        this.queryPageList = queryPageList;
        for(QueryPage queryPage : queryPageList){
            registerQueryPage(queryPage);
        }
    }

    public DatabaseType getDatabaseType(){
        if(dataSource == null){
            return DatabaseType.NONE;
        }
        DataSource current = dataSource;
        if(dataSource instanceof DynamicDataSource){
            DynamicDataSource dynamicDataSource = (DynamicDataSource) dataSource;
            current = dynamicDataSource.currentDataSource();
        }
        DatabaseType type = databaseTypeMap.get(current);
        if(type != null){
            return type;
        }
        Connection connection = null;
        try {
            connection = current.getConnection();
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
            databaseTypeMap.put(current, databaseType);
            return databaseType;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(connection, current);
        }
        return DatabaseType.NONE;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public QueryPage getQueryPage(){
        DatabaseType databaseType =  getDatabaseType();
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
            }
            queryPageMap.put(databaseType, queryPage);
        }
        return queryPage;
    }

    public void registerQueryPage(QueryPage queryPage){
        queryPageMap.put(queryPage.databaseType(), queryPage);
    }

    public synchronized void close(ResultSet rs, Statement stmt, Connection conn, DataSource ds){
        try {
            if(rs != null){
                rs.close();
            }
            if(stmt != null){
                stmt.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            DataSourceUtils.releaseConnection(conn, ds);
        }
    }

    public synchronized void close(Statement stmt, Connection conn, DataSource ds){
        close(null, stmt, conn, ds);
    }

    public synchronized void close(Connection conn, DataSource ds){
        close(null, null, conn, ds);
    }

}
