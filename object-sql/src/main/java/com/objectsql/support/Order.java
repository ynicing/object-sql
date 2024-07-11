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

public class Order implements Serializable {

    public static final String ASC = "ASC";
    public static final String DESC = "DESC";
		
    private Column column;
    private String order;

    public Order(Column column, String order){
        this.order = order;
        this.column = column;
    }

    public Order(String column, String order){
        this(new Column(column), order);
    }

    public <T,R> Order(LambdaQuery<T,R> lambdaQuery, String order){
        this(lambdaQuery.getColumnName(), order);
    }

    public static Order asc(Column column){
        return new Order(column, ASC);
    }

    public static Order desc(Column column){
        return new Order(column, DESC);
    }

    public static Order create(Column column, String order){
        return new Order(column, order);
    }

    public Column getColumn() {
        return column;
    }

    public void setColumn(Column column) {
        this.column = column;
    }

    public String getOrder() {
        return order;
    }
    public void setOrder(String order) {
        this.order = order;
    }


}