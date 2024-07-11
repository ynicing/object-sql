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

import com.objectsql.query.QueryUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Names {

    private List<String> _names = new ArrayList<String>();

    public String [] names(){
        return _names.toArray(new String[_names.size()]);
    }

    public Names(String ... names){
        if(names != null){
            _names.addAll(Arrays.asList(names));
        }
    }

    public static Names create(String ... names){
        return new Names(names);
    }

    public static <T,R> Names lambdaCreate(LambdaQuery<T,R> ... lambdaQueries){
        return new Names(QueryUtils.getColumns(lambdaQueries));
    }

    public Names name(String name){
        _names.add(name);
        return this;
    }

    public <T,R> Names name(LambdaQuery<T,R> lambdaQuery){
        _names.add(lambdaQuery.getColumnName());
        return this;
    }
}
