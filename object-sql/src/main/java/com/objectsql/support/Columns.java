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

import com.objectsql.exception.ORMException;
import com.objectsql.query.QueryUtils;
import com.objectsql.utils.ORMUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class Columns implements Serializable {

    //as

	private List<Column> columns = new ArrayList<Column>();

    public Columns(){}

    private String alias;
    private String [] names;
    private String [] asNames;
    private String [] fieldNames;

    private String asNamePrefix;

    public String getAsNamePrefix() {
        return asNamePrefix;
    }

    public void setAsNamePrefix(String asNamePrefix) {
        this.asNamePrefix = asNamePrefix;
    }

    public Columns(String alias){
        this.alias = alias;
    }

	public Columns(String alias, String ... names){
        this.alias = alias;
        this.names = names;
    }


    public Columns names(String ... names){
        this.names = names;
        return this;
    }

    public <T,R> Columns lambdaQueryNames(LambdaQuery<T,R> ... lambdaQueries){
        this.names = QueryUtils.getColumns(lambdaQueries);
        return this;
    }

    public Columns asNames(String ... asNames){
        this.asNames = asNames;
        return this;
    }

    public Columns asNamePrefix(String asNamePrefix){
        this.asNamePrefix = asNamePrefix;
        return this;
    }

    public Columns fieldNames(String ... fieldNames){
        this.fieldNames = fieldNames;
        return this;
    }

    public Columns c(Column column){
        if(column != null){
           if(ORMUtils.isEmpty(column.getAlias()) && !ORMUtils.isEmpty(alias)){
               column.setAlias(alias);
           }
           columns.add(column);
        }
        return this;
    }

    public List<Column> getColumnList() {
        List<Column> columnList = new ArrayList<Column>();
        if(names != null && alias != null){
            String prefix = asNamePrefix == null?"":asNamePrefix;
            if(asNames == null){
                if(fieldNames == null) {
                    for (String name : names) {
                        if(ORMUtils.isEmpty(prefix)) {
                            columnList.add(new Column(alias, name));
                        }else{
                            columnList.add(new Column(alias, name).as(prefix + name));
                        }
                    }
                }else{
                    if(names.length == fieldNames.length){
                        for(int i = 0; i < names.length; i++){
                            if(ORMUtils.isEmpty(prefix)) {
                                columnList.add(new Column(alias, names[i]).fieldName(fieldNames[i]));
                            }else{
                                columnList.add(new Column(alias, names[i]).as(prefix + names[i]).fieldName(fieldNames[i]));
                            }
                        }
                    }else{
                        throw new ORMException("TABLE_QUERY_NAMES_AS_NOT_EQUAL, names length: " +
                                names.length + ", fieldNames length : " + fieldNames.length);
                    }
                }
            }else{
                if(names.length == asNames.length){
                    if(fieldNames != null) {
                        if (names.length == fieldNames.length) {
                            for (int i = 0; i < names.length; i++) {
                                columnList.add(new Column(alias, names[i], prefix + asNames[i]).fieldName(fieldNames[i]));
                            }
                        } else {
                            throw new ORMException("TABLE_QUERY_NAMES_AS_NOT_EQUAL, names length: " +
                                    names.length + ", fieldNames length : " + fieldNames.length);
                        }
                    }else {
                        for (int i = 0; i < names.length; i++) {
                            columnList.add(new Column(alias, names[i], prefix + asNames[i]));
                        }
                    }
                }else{
                    throw new ORMException("TABLE_QUERY_NAMES_AS_NOT_EQUAL, names length: " +
                            names.length + ", asNames length : " + asNames.length);
                }
            }
        }
        for(Column col : columns) {
            if(col instanceof CaseColumn){
                CaseColumn column = new CaseColumn(col.getFunction(), col.getAlias(), col.getName(), col.getAsName());
                column.setFieldName(col.getFieldName());
                column.setFormat(col.getFormat());
                column.setOperatorInFunction(col.getOperatorInFunction());
                column.setQuery(col.getQuery());
                column.setType(col.getType());
                column.setValue(col.getValue());
                CaseColumn caseColumn = (CaseColumn) col;
                column.setConditions(caseColumn.getConditions());
                column.setElseValue(caseColumn.getElseValue());
                columnList.add(column);
            }else {
                Column column = new Column(col.getFunction(), col.getAlias(), col.getName(), col.getAsName());
                column.setFieldName(col.getFieldName());
                column.setFormat(col.getFormat());
                column.setOperatorInFunction(col.getOperatorInFunction());
                column.setQuery(col.getQuery());
                column.setType(col.getType());
                column.setValue(col.getValue());
                if (!ORMUtils.isEmpty(asNamePrefix)) {
                    if (ORMUtils.isEmpty(column.getAsName())) {
                        column.setAsName(asNamePrefix + column.getName());
                    }
                    if (!ORMUtils.isEmpty(column.getFieldName())) {
                        column.setFieldName(QueryUtils.displayName(asNamePrefix) +
                                column.getFieldName().substring(0, 1).toUpperCase(Locale.ROOT) +
                                column.getFieldName().substring(1));
                    }
                }
                columnList.add(column);
            }
        }
        return columnList;
    }

}