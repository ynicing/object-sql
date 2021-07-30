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

import java.util.Collection;
import java.util.List;

public interface IBaseQuery extends IQuery{

	//从class中获取字段，该字段可有可无
	IBaseQuery table(Class<?> clazz);
    IBaseQuery where(String name, ExpressionType type);

	IBaseQuery whereEqual(String name, Object value);
	IBaseQuery whereNotEqual(String name, Object value);

	IBaseQuery whereLike(String name, String value);
	IBaseQuery whereNotLike(String name, String value);
	IBaseQuery whereStartWith(String name, String value);
	IBaseQuery whereEndWith(String name, String value);
	IBaseQuery whereNotStartWith(String name, String value);
	IBaseQuery whereNotEndWith(String name, String value);

	IBaseQuery whereLess(String name, Object value);
	IBaseQuery whereLessEqual(String name, Object value);
	IBaseQuery whereMore(String name, Object value);
	IBaseQuery whereMoreEqual(String name, Object value);

	IBaseQuery whereIn(String name, Collection value);
	IBaseQuery whereNotIn(String name, Collection value);

	IBaseQuery whereInValues(String name, Object ... values);
	IBaseQuery whereNotInValues(String name, Object ... values);

	IBaseQuery whereIsNull(String name);
    IBaseQuery whereIsNotNull(String name);
	IBaseQuery whereIsEmpty(String name);
	IBaseQuery whereIsNotEmpty(String name);
	IBaseQuery where(String name, Object value, ExpressionType type);
	IBaseQuery where(Terms terms);//select * from test where (a = ? or b = ? ...)

	IBaseQuery where(Express... expresses);
	IBaseQuery where(Expression... expressions);

	IBaseQuery whereBetween(String name, Object value, Object andValue);

	IBaseQuery group(String name);
	IBaseQuery groupCountSelectColumn(String name);
	IBaseQuery having(String name, Object value, ExpressionType type);
	IBaseQuery having(Terms terms);//group by  having (a = ? or b = ? ...)
	IBaseQuery orderDesc(String name);
	IBaseQuery orderAsc(String name);
    IBaseQuery orders(List<Order> orders);
	IBaseQuery createQuery(String... names);
    IBaseQuery createQuery(Class<?> clazz, String... names);
    IBaseQuery createQuery(Class<?> clazz, Column... columns);
	IBaseQuery addReturnColumn(Column column);
	IBaseQuery addReturnColumn(String column);
	IBaseQuery clearReturnColumns();

    IBaseQuery distinct();

}
