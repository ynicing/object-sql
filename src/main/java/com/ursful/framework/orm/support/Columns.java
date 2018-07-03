/*
 * Copyright 2017 @ursful.com
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

package com.ursful.framework.orm.support;

import com.ursful.framework.orm.error.ORMErrorCode;
import com.ursful.framework.core.exception.CommonException;

import java.util.ArrayList;
import java.util.List;

public class Columns {

    //as

	private List<Column> columnList = new ArrayList<Column>();

    public Columns(){}

    private String alias;
    private String [] names;
    private String [] asNames;

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

    public Columns asNames(String ... asNames){
        this.asNames = asNames;
        return this;
    }

    public List<Column> getColumnList() {
        columnList.clear();
        if(names != null && alias != null){
            if(asNames == null){
                for(String name : names){
                    columnList.add(new Column(alias, name));
                }
            }else{
                if(names.length == asNames.length){
                    for(int i = 0; i < names.length; i++){
                        columnList.add(new Column(alias, names[i], asNames[i]));
                    }
                }else{
                    throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.TABLE_QUERY_NAMES_AS_NOT_EQUAL, "names length: " +
                            names.length + ", asNames length : " + asNames.length);
                }
            }
        }
        return columnList;
    }

    public void setColumnList(List<Column> columnList) {
        this.columnList = columnList;
    }
}