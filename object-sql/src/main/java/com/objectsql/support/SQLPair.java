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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SQLPair  implements Serializable {

    private String sql;
    private List<Pair> pairs;

    public SQLPair(){}

    public SQLPair(String sql){
        this.sql = sql;
        this.pairs = new ArrayList<Pair>();
    }

    public SQLPair(String sql, Pair pair){
        this.sql = sql;
        this.pairs = new ArrayList<Pair>();
        this.pairs.add(pair);
    }

    public SQLPair(String sql, List<Pair> pairs){
        this.sql = sql;
        this.pairs = pairs;
    }

    public static SQLPair create(String sql){
        return new SQLPair(sql);
    }

    public static SQLPair create(String sql, Pair pair){
        return new SQLPair(sql, pair);
    }

    public static SQLPair create(String sql, List<Pair> pairs){
        return new SQLPair(sql, pairs);
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }


    public List<Pair> getPairs() {
        return pairs;
    }

    public void setPairs(List<Pair> pairs) {
        this.pairs = pairs;
    }
}
