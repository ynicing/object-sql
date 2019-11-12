package com.ursful.framework.orm.support;

import com.ursful.framework.orm.utils.ORMUtils;

import java.io.Serializable;
import java.util.List;

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

    private String id;

    public Column getIdColumn(){
        return new Column(alias, getId());
    }

    public String getId(){
        if(id == null) {
            ColumnInfo columnInfo = getColumnInfoId();
            if (columnInfo != null) {
                return columnInfo.getColumnName();
            }
        }
        return id;
    }

    public ColumnInfo getColumnInfoId() {
        ColumnInfo columnInfoId = null;
        if(table instanceof Class) {
            List<ColumnInfo> infoList = ORMUtils.getColumnInfo((Class) table);
            for (ColumnInfo info : infoList) {
                if (info.getPrimaryKey()) {
                    columnInfoId = info;
                    break;
                }
            }
        }
        return columnInfoId;
    }
}
