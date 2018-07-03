package com.ursful.framework.core.datasource;

import org.springframework.jdbc.datasource.lookup.AbstractRoutingDataSource;

import javax.sql.DataSource;

/**
 * 类名：DynamicDataSource
 * 创建者：huangyonghua
 * 日期：2017-07-17 16:28
 * 版权：厦门维途信息技术有限公司 Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class DynamicDataSource extends AbstractRoutingDataSource{

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
