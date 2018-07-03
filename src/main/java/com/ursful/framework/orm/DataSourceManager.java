package com.ursful.framework.orm;

import javax.sql.DataSource;

/**
 * Created by ynice on 27/06/2018.
 */
public class DataSourceManager {

    private DataSource dataSource;

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }


}
