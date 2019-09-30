package com.ursful.framework.orm.support;

import java.io.Serializable;

public class AliasTable implements Serializable {

    private String alias;

    public Column columns(){
        return new Column(alias, Expression.EXPRESSION_ALL);
    }

    private Object table;

    public Object getTable(){
        return table;
    }

    public void setAlias(String alias){
        this.alias = alias;
    }

    public Columns cs(String ... names){
        return new Columns(alias, names);
    }

    public Column c(String name){
        return new Column(alias, name);
    }

    public Column c(String name, String asName){
        return new Column(alias, name, asName);
    }

    public Column c(String function, String name, String asName){
        return new Column(function, alias, name, asName);
    }

    public String getAlias(){
        return alias;
    }

    public AliasTable(){}

    public AliasTable(Object table){
        this.table = table;
    }


}
