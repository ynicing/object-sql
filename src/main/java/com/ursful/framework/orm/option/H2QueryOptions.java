package com.ursful.framework.orm.option;

public class H2QueryOptions extends MySQLOptions{
    @Override
    public String keyword() {
        return "h2";
    }

    @Override
    public String databaseType() {
        return "H2";
    }

}
