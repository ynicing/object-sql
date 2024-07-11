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
package com.objectsql;


import com.objectsql.handler.IQueryConvert;
import com.objectsql.support.*;

import java.util.List;

public interface IQuery{

//    Map<String, IQuery> getAliasQuery();
    String id();

    void setId(String id);

    Class<?> getTable();

    boolean isDistinct();

    List<Condition> getConditions();

    List<Condition> getHavings();

    List<Column> getGroups();
    //快速group
    List<Column> getGroupCountSelectColumns();

    List<Order> getOrders();

    Class<?> getReturnClass();

    List<Column> getReturnColumns();
    List<Column> getFinalReturnColumns();
    List<Column> getFixedReturnColumns();
    Pageable getPageable();

    void setPageable(Pageable pageable);

    void setOptions(Options options);

    Options getOptions();

    QueryInfo doQuery();

    QueryInfo doQueryCount();

    void setDataPermission(boolean allowed);

    boolean dataPermission();

    TextTransformType textTransformType();

    void setTextTransformType(TextTransformType textTransformType);

    //可能产生的问题，Express 不能共用，否则 可能引起时间不一致的问题
    boolean isLessEqualDatePlus235959();
    boolean isLessDatePlus235959();

    void enableLessOrLessEqualDatePlus235959();
    void enableLessDatePlus235959();
    void enableLessEqualDatePlus235959();

    void setQueryConvert(IQueryConvert queryConvert);
    IQueryConvert getQueryConvert();
}
