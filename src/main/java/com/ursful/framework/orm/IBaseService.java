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


import com.ursful.framework.orm.listener.IChangeListener;
import com.ursful.framework.orm.listener.IDefaultListener;
import com.ursful.framework.orm.listener.IORMListener;
import com.ursful.framework.orm.listener.IServiceChangedListener;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.support.Terms;

import java.util.List;

public interface IBaseService<T> extends IServiceChangedListener<T>, ISQLService{

    void setDataSourceManager(DataSourceManager dataSourceManager);

    void addDefaultListener(IDefaultListener listener);
    void removeDefaultListener(IDefaultListener listener);
    void addChangeListener(IChangeListener listener);
    void removeChangeListener(IChangeListener listener);
    void addORMListener(IORMListener listener);
    void removeORMListener(IORMListener listener);

    /*  查询一个对象 方式一 :  取条件第一个值
        Test tmp = new Test();
        tmp.setId(1);//主键查询
        tmp.setType(3);
        Test test  = baseDao.get(tmp);

        查询一个对象(按主键) 方式二
        Test test  = baseDao.get(1);
        Test test  = baseDao.get(new Integer(1));

        取第一个 方式三
        Test tmp = new Test();
        tmp.setType(3);
        Test test  = baseDao.get(tmp);

        方式四 Query
        IBaseQuery query = new BaseQueryImpl();
        query.table(Test.class).createQuery();
        test = testService.get(query);

        IMultiQuery multiQuery = new MultiQueryImpl();
        multiQuery.table("a", Test.class);
        multiQuery.where(new Column("a", Test.T_ID), 3, ExpressionType.CDT_More);
        multiQuery.createQuery(Test.class);
        test = testService.get(multiQuery);

    */
    <S> S get(Object object);

    /*  保存
        testService.save(test);
    */
    boolean save(T t);
    boolean saveWithoutListener(T t);

    /*  更新（按主键), 若对象字段为空则不更新
        testService.update(test);
    */
    boolean update(T t);
    boolean updateWithoutListener(T t);
    /*  更新（按主键), 若对象字段为空也更新
        testService.update(test, true);
    */
    boolean update(T t, boolean updateNull);
    boolean updateWithoutListener(T t, boolean updateNull);

    boolean updateNull(T t, String ...forNullColumns);
    boolean updateNullWithoutListener(T t, String ...forNullColumns);

    boolean updates(T t, Express ... expresses);
    boolean updatesWithoutListener(T t, Express ... expresses);

    boolean updatesNull(T t, Express ... expresses);
    boolean updatesNullWithoutListener(T t, Express ... expresses);

    boolean updatesNull(T t, String [] forNullColumns,  Express ... expresses);
    boolean updatesNullWithoutListener(T t, String [] forNullColumns, Express ... expresses);


    boolean updates(T t, Terms terms);
    boolean updatesWithoutListener(T t, Terms terms);

    boolean updatesNull(T t, Terms terms);
    boolean updatesNullWithoutListener(T t, Terms terms);

    boolean updatesNull(T t, String [] forNullColumns,  Terms terms);
    boolean updatesNullWithoutListener(T t, String [] forNullColumns, Terms terms);

    /* 删除, (限制条件删除，危险的动作)
       testService.delete(test);
       删除（按主键)
       baseDao.delete(1, Test.class);
    */
    boolean delete(Object object);
    boolean deleteWithoutListener(Object object);

    boolean deletes(Terms terms);
    boolean deletesWithoutListener(Terms terms);

    boolean deletes(Express... expresses);
    boolean deletesWithoutListener(Express... expresses);

    //查询该表总数据
    int count(Express ... expresses);
    boolean exists(Terms terms);
    boolean exists(Express ... expresses);

    //简单的查询(单表）
    List<T> listNames(String ...names);
    List<T> list();
    List<T> list(int start, int size);
    List<T> list(Express ... expresses);
    List<T> list(Terms terms);
    List<T> list(Terms terms, MultiOrder multiOrder);
    List<T> list(Terms terms, MultiOrder multiOrder, Integer limit);
    List<T> list(Names names, Terms terms);
    List<T> list(Names names, Terms terms, MultiOrder multiOrder);
    List<T> list(Names names, Terms terms, MultiOrder multiOrder, Integer limit);

    //复杂的查询（多表）, 可以返回Bean，ExtBean，Map类型
    <S> List<S> query(IQuery query);//queryDistinctString
    <S> List<S> query(IQuery q, int size);
    int queryCount(IQuery query);
    <S> Pageable<S> queryPage(IQuery query, Pageable page);


}
