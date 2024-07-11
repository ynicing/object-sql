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

import com.objectsql.query.BaseQueryImpl;
import com.objectsql.support.*;

import java.util.Collection;
import java.util.List;

public interface IBaseQuery extends IQuery{

	//从class中获取字段，该字段可有可无
	IBaseQuery table(Class<?> clazz);
	IBaseQuery where(String name, ExpressionType type);
	<T,R> IBaseQuery where(LambdaQuery<T,R> fieldFunction, ExpressionType type);

	IBaseQuery whereEqual(String name, Object value);
	IBaseQuery whereNotEqual(String name, Object value);
	<T,R> IBaseQuery whereEqual(LambdaQuery<T,R> fieldFunction, Object value);
	<T,R> IBaseQuery whereNotEqual(LambdaQuery<T,R> fieldFunction, Object value);

	IBaseQuery whereLike(String name, String value);
	IBaseQuery whereNotLike(String name, String value);
	IBaseQuery whereStartWith(String name, String value);
	IBaseQuery whereEndWith(String name, String value);
	IBaseQuery whereNotStartWith(String name, String value);
	IBaseQuery whereNotEndWith(String name, String value);

	<T,R> IBaseQuery whereLike(LambdaQuery<T,R> fieldFunction, String value);
	<T,R> IBaseQuery whereNotLike(LambdaQuery<T,R> fieldFunction, String value);
	<T,R> IBaseQuery whereStartWith(LambdaQuery<T,R> fieldFunction, String value);
	<T,R> IBaseQuery whereEndWith(LambdaQuery<T,R> fieldFunction, String value);
	<T,R> IBaseQuery whereNotStartWith(LambdaQuery<T,R> fieldFunction, String value);
	<T,R> IBaseQuery whereNotEndWith(LambdaQuery<T,R> fieldFunction, String value);

	IBaseQuery whereLess(String name, Object value);
	IBaseQuery whereLessEqual(String name, Object value);
	IBaseQuery whereMore(String name, Object value);
	IBaseQuery whereMoreEqual(String name, Object value);

	<T,R> IBaseQuery whereLess(LambdaQuery<T,R> fieldFunction, Object value);
	<T,R> IBaseQuery whereLessEqual(LambdaQuery<T,R> fieldFunction, Object value);
	<T,R> IBaseQuery whereMore(LambdaQuery<T,R> fieldFunction, Object value);
	<T,R> IBaseQuery whereMoreEqual(LambdaQuery<T,R> fieldFunction, Object value);

	IBaseQuery whereIn(String name, Collection value);
	IBaseQuery whereNotIn(String name, Collection value);
	IBaseQuery whereInValues(String name, Object ... values);
	IBaseQuery whereNotInValues(String name, Object ... values);

	<T,R> IBaseQuery whereIn(LambdaQuery<T,R> fieldFunction, Collection value);
	<T,R> IBaseQuery whereNotIn(LambdaQuery<T,R> fieldFunction, Collection value);
	<T,R> IBaseQuery whereInValues(LambdaQuery<T,R> fieldFunction, Object ... values);
	<T,R> IBaseQuery whereNotInValues(LambdaQuery<T,R> fieldFunction, Object ... values);

	IBaseQuery whereIsNull(String name);
    IBaseQuery whereIsNotNull(String name);
	IBaseQuery whereIsEmpty(String name);
	IBaseQuery whereIsNotEmpty(String name);
	IBaseQuery where(String name, Object value, ExpressionType type);

	<T,R> IBaseQuery whereIsNull(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery whereIsNotNull(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery whereIsEmpty(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery whereIsNotEmpty(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery where(LambdaQuery<T,R> fieldFunction, Object value, ExpressionType type);

	IBaseQuery where(Condition condition);//select * from test where (a = ? or b = ? ...)

	IBaseQuery where(Expression... expressions);

	IBaseQuery whereBetween(String name, Object value, Object andValue);
	<T,R> IBaseQuery whereBetween(LambdaQuery<T,R> fieldFunction, Object value, Object andValue);

	IBaseQuery group(String name);
	IBaseQuery groupCountSelectColumn(String name);
	IBaseQuery having(String name, Object value, ExpressionType type);

	<T,R> IBaseQuery group(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery groupCountSelectColumn(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery having(LambdaQuery<T,R> fieldFunction, Object value, ExpressionType type);

	IBaseQuery having(Condition condition);//group by  having (a = ? or b = ? ...)

	IBaseQuery orderDesc(String name);
	IBaseQuery orderAsc(String name);
	<T,R> IBaseQuery orderDesc(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery orderAsc(LambdaQuery<T,R> fieldFunction);

	IBaseQuery order(Order order);
    IBaseQuery orders(List<Order> orders);

	IBaseQuery createQuery(String... names);
	IBaseQuery createQuery(Class<?> clazz, String... names);

	<T,R> IBaseQuery createQuery(LambdaQuery<T,R>... names);
	<T,R> IBaseQuery createQuery(Class<?> clazz, LambdaQuery<T,R>... names);

    IBaseQuery createQuery(Class<?> clazz, Column... columns);
	IBaseQuery addReturnColumn(Column column);
	IBaseQuery addReturnColumn(String column);
	IBaseQuery clearReturnColumns();
	IBaseQuery addFixedReturnColumn(Column column);
	IBaseQuery addFixedReturnColumn(String column);
	IBaseQuery clearFixedReturnColumns();

	<T,R> IBaseQuery addReturnColumn(LambdaQuery<T,R> fieldFunction);
	<T,R> IBaseQuery addFixedReturnColumn(LambdaQuery<T,R> fieldFunction);


	IBaseQuery distinct();

	static IBaseQuery newBaseQuery(){
		return new BaseQueryImpl();
	}

}
