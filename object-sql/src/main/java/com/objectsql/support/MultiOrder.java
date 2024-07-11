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

public class MultiOrder implements Serializable {

    private List<Order> orders = new ArrayList<Order>();

    public void addOrder(Order order){
        this.orders.add(order);
    }

    public static MultiOrder create(){
        return new MultiOrder();
    }

    public MultiOrder(){
    }

    public MultiOrder(Column column, String order){
        Order temp = new Order(column, order);
        orders.add(temp);
    }

    public MultiOrder desc(Column column){
        Order temp = new Order(column, Order.DESC);
        orders.add(temp);
        return this;
    }

    public MultiOrder asc(Column column){
        Order temp = new Order(column, Order.ASC);
        orders.add(temp);
        return this;
    }

    public MultiOrder desc(String column){
        Order temp = new Order(new Column(column), Order.DESC);
        orders.add(temp);
        return this;
    }

    public MultiOrder asc(String column){
        Order temp = new Order(new Column(column), Order.ASC);
        orders.add(temp);
        return this;
    }

    public <T,R> MultiOrder desc(LambdaQuery<T,R> lambdaQuery){
        Order temp = new Order(new Column(lambdaQuery.getColumnName()), Order.DESC);
        orders.add(temp);
        return this;
    }

    public <T,R> MultiOrder asc(LambdaQuery<T,R> lambdaQuery){
        Order temp = new Order(new Column(lambdaQuery.getColumnName()), Order.ASC);
        orders.add(temp);
        return this;
    }

    public List<Order> getOrders() {
        return this.orders;
    }

    public void setOrders(List<Order> orders) {
        this.orders = orders;
    }
}