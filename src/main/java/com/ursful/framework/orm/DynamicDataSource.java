package com.ursful.framework.orm;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;

public class DynamicDataSource extends AbstractRoutingDataSource {

    private static final ThreadLocal<String> contextHolder = new ThreadLocal<String>();

    public static void setDataSource(String dataSource){
        contextHolder.set(dataSource);
    }

    public static String getDataSource(){
        return contextHolder.get();
    }

    @Override
    protected Object determineCurrentLookupKey() {
        return getDataSource();
    }

    public DataSource currentDataSource() {
        return determineTargetDataSource();
    }
}
