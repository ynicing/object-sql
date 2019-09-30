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
package com.ursful.framework.orm;


import com.ursful.framework.orm.support.*;

import java.util.List;
import java.util.Map;

public interface IQuery{

    Map<String, IQuery> getAliasQuery();

    Class<?> getTable();

    boolean isDistinct();

    List<String> getAliasList();

    Map<String, Class<?>> getAliasTable();

    List<Join> getJoins();

    List<Condition> getConditions();

    List<Condition> getHavings();

    List<Column> getGroups();

    List<Order> getOrders();

    Class<?> getReturnClass();

    List<Column> getReturnColumns();

    Pageable getPageable();

    void setPageable(Pageable pageable);

    void setOptions(Options options);

    Options getOptions();

    QueryInfo doQuery();

    QueryInfo doQueryCount();


}
