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


import com.objectsql.query.MultiQueryImpl;
import com.objectsql.support.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IMultiQuery extends IQuery{

    static IMultiQuery newMultiQuery(){
        return new MultiQueryImpl();
    }

    IMultiQuery createQuery(Class<?> clazz, Column... columns);//select a.id, a.name from
    IMultiQuery createQuery(Class<?> clazz, Columns... columns);//select a.id, a.name from
    IMultiQuery createQuery(Column ... columns);

    IMultiQuery addReturnColumn(Column column);
    IMultiQuery clearReturnColumns();

    IMultiQuery addFixedReturnColumn(Column column);
    IMultiQuery clearFixedReturnColumns();

    AliasTable table(IQuery query);
    AliasTable table(Class<?> clazz);
    AliasTable table(String table);
    AliasTable table(String table, String alias);
    AliasTable table(IQuery query, String alias);
    AliasTable table(Class<?> clazz, String alias);

    AliasTable join(IQuery query);
    AliasTable join(Class<?> clazz);
    AliasTable join(IQuery query, String alias);
    AliasTable join(Class<?> clazz, String alias);

    IMultiQuery whereEqual(Column left, Object value);
    IMultiQuery whereNotEqual(Column left, Object value);

    IMultiQuery whereLike(Column left, String value);
    IMultiQuery whereNotLike(Column left, String value);
    IMultiQuery whereStartWith(Column left, String value);
    IMultiQuery whereEndWith(Column left, String value);
    IMultiQuery whereNotStartWith(Column left, String value);
    IMultiQuery whereNotEndWith(Column left, String value);

    IMultiQuery whereLess(Column left, Object value);
    IMultiQuery whereLessEqual(Column left, Object value);
    IMultiQuery whereMore(Column left, Object value);
    IMultiQuery whereMoreEqual(Column left, Object value);

    IMultiQuery whereIsNull(Column left);
    IMultiQuery whereIsNotNull(Column left);
    IMultiQuery whereIsEmpty(Column left);
    IMultiQuery whereIsNotEmpty(Column left);

    IMultiQuery whereIn(Column left, Collection value);
    IMultiQuery whereNotIn(Column left, Collection value);

    IMultiQuery whereInValues(Column left, Object ... values);
    IMultiQuery whereNotInValues(Column left, Object ... values);

    IMultiQuery whereInQuery(Column left, IMultiQuery query);
    IMultiQuery whereNotInQuery(Column left, IMultiQuery query);

    IMultiQuery whereBetween(Column left, Object value, Object andValue);

    IMultiQuery where(Column left, Object value, ExpressionType type);
    IMultiQuery where(Column left, Column value);
    IMultiQuery where(Condition condition);
    IMultiQuery where(Expression... expressions);
    IMultiQuery group(Column ...columns);
    IMultiQuery group(Columns ...columns);
    IMultiQuery groupCountSelectColumn(Column ... columns);

    IMultiQuery having(Column left, Object value, ExpressionType type);
    IMultiQuery having(Column left, Column value);
    IMultiQuery having(Condition condition);
    IMultiQuery having(Expression expression);

    IMultiQuery orderDesc(Column column);
    IMultiQuery orderAsc(Column column);
    IMultiQuery order(Order order);
    IMultiQuery orders(List<Order> orders);

    IMultiQuery join(Join join);
    IMultiQuery join(AliasTable table, Column left, Column right);
    IMultiQuery join(AliasTable table, JoinType joinType, Column left, Column right);
    IMultiQuery distinct();

    IMultiQuery enableDataPermission();
    IMultiQuery disableDataPermission();
    boolean isEnableDataPermission();

    String dataKey(Object dataType);
    IMultiQuery dataColumn(Object dataType, Column column);
    IMultiQuery dataColumn(Object dataType, String dataKey, Column column);
    Column dataColumn(Object dataType);

    boolean containsAlias(String alias);
    void addUsedAlias(String alias);

    IMultiQuery whereExists(IMultiQuery query);
    IMultiQuery whereNotExists(IMultiQuery query);

    IMultiQuery createMultiQuery();

    IMultiQuery parentQuery();

    List<String> getAliasList();

    Map<String, Object> getAliasTable();
    List<Join> getJoins();

//    IMultiQuery union(IMultiQuery query);
//    IMultiQuery unionAll(IMultiQuery query);
}
