/*
 * Copyright 2017 @objectsql.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.objectsql.support;

import com.objectsql.IQuery;
import com.objectsql.query.QueryUtils;
import com.objectsql.utils.ORMUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class AliasTable implements Serializable {

    private String alias;

    public Column columns(){
        return new Column(alias, Column.ALL);
    }

    private Object table;

    public Object getTable(){
        return table;
    }

    public void setAlias(String alias){
        this.alias = alias;
    }

    public Columns all(){
        return allExclude();
    }

    public <T,R> Columns allLambdaExclude(LambdaQuery<T,R> ... lambdaQueries){
        return allExclude(QueryUtils.getColumns(lambdaQueries));
    }

    public Columns allExclude(String ... columns){
        List<String> exclude = new ArrayList<String>();
        if(columns != null){
            for (String column : columns){
                exclude.add(column);
            }
        }
        Columns cs = new Columns(this.alias);
        if(table instanceof Class<?>){
            List<ColumnInfo> infoList = ORMUtils.getColumnInfo((Class<?>)table);
            if(infoList != null){
                for (ColumnInfo columnInfo : infoList){
                    if (!exclude.contains(columnInfo.getColumnName())){
                        cs.c(new Column(columnInfo.getColumnName()).fieldName(columnInfo.getName()));
                    }
                }
            }
        }else if(table instanceof IQuery){
            //子查询，不需要fixedColumn
            List<Column>  list = ((IQuery)table).getReturnColumns();
            if(list != null){
                for (Column c : list){
                    if(!exclude.contains(c.getName())) {
                        cs.c(c);
                    }
                }
            }
        }
        return cs;
    }



    public Columns cs(String ... names){
        return new Columns(alias, names);
    }

    public <T,R> Columns lambdaCs(LambdaQuery<T,R> ... lambdaQueries){
        return new Columns(alias, QueryUtils.getColumns(lambdaQueries));
    }

    public Columns cs(Column column){
        return new Columns(alias).c(column);
    }
    public Column c(IQuery query){
        return new Column(query);
    }
    public Column c(Column column){
        return column.alias(alias);
    }

    public Column c(String name){
        return new Column(alias, name);
    }

    public <T,R> Column c(LambdaQuery<T,R> lambdaQuery){
        return new Column(alias, lambdaQuery.getColumnName());
    }

    public Column c(String name, String asName){
        return new Column(alias, name, asName);
    }

    public <T,R> Column c(LambdaQuery<T,R> lambdaQuery, String asName){
        return new Column(alias, lambdaQuery.getColumnName(), asName);
    }

    public Column c(String function, String name, String asName){
        return new Column(function, alias, name, asName);
    }

    public <T,R> Column c(String function, LambdaQuery<T,R> lambdaQuery, String asName){
        return new Column(function, alias, lambdaQuery.getColumnName(), asName);
    }

    public String getAlias(){
        return alias;
    }

    public AliasTable(){}

    public AliasTable(Class<?> clazz){
        this.table = clazz;
    }

    public AliasTable(IQuery query){
        this.table = query;
    }

    public AliasTable(String table){
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
