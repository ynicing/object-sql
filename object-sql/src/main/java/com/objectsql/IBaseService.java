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


import com.objectsql.exception.ORMException;
import com.objectsql.listener.*;
import com.objectsql.support.*;

import java.util.List;

public interface IBaseService<T> extends IServiceChangedListener<T>, ISQLService{

    void setObjectSQLManager(ObjectSQLManager objectSQLManager);

    void addDefaultListener(IDefaultListener listener);
    void removeDefaultListener(IDefaultListener listener);
    void addChangeListener(IChangeListener listener);
    void removeChangeListener(IChangeListener listener);
    void addORMListener(IORMListener listener);
    void removeORMListener(IORMListener listener);
    void addChangedListener(IChangedListener listener);
    void removeChangedListener(IChangedListener listener);
    void addQueryListener(IQueryListener listener);
    void removeQueryListener(IQueryListener listener);

    List<IORMListener> getListeners();
    void setListeners(List<IORMListener> listeners);
    List<IDefaultListener> getDefaultListeners();
    void setDefaultListeners(List<IDefaultListener> defaultListeners);
    List<IChangeListener> getChangeListeners();
    void setChangeListeners(List<IChangeListener> changeListeners);
    List<IChangedListener> getChangedListeners();
    void setChangedListeners(List<IChangedListener> changedListeners);
    List<IQueryListener> getQueryListeners();
    void setQueryListeners(List<IQueryListener> queryListeners);
    void copyAllListeners(IBaseService service);

    //触发默认值监听器
    void triggerDefaultListener(ORMType type, T t);
    //触发ORM监听器
    void triggerORMListener(ORMType type, T t);
    void triggerORMListener(ORMType type, T t, boolean updateNull, String[] nullColumns);
    //事务启用时，异常则回滚
    void triggerChangeListener(ORMType ormType, final T original, final T t);
    void triggerChangeListener(ORMType ormType, final T original, final T t, boolean updateNull, String [] nullColumns);
    //立即触发
    void triggerChangedListenerImmediately(ORMType ormType, final T original, final T current);
    void triggerChangedListenerImmediately(ORMType ormType, final T original, final T current, boolean updateNull, String[] nullColumns);
    //当事务结束时触发
    void triggerChangedListenerWhenTransactionFinish(IServiceChangedListener serviceChangedListener, ORMType ormType, T original, T current);
    void triggerChangedListenerWhenTransactionFinish(IServiceChangedListener serviceChangedListener, ORMType ormType, T original, T current, boolean updateNull, String[] nullColumns);

//    Class getBaseClass();

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
    boolean insert(T t);
    boolean insertWithoutListener(T t);

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

    <P,R> boolean updateLambdaNull(T t, LambdaQuery<P,R> ...forNullColumns);
    <P,R> boolean updateLambdaNullWithoutListener(T t, LambdaQuery<P,R>  ...forNullColumns);

    boolean updates(T t, Expression... expressions);
    boolean updatesWithoutListener(T t, Expression ... expressions);

    boolean updatesNull(T t, Expression ... expressions);
    boolean updatesNullWithoutListener(T t, Expression ... expressions);

    boolean updatesNull(T t, String [] forNullColumns,  Expression ... expressions);
    boolean updatesNullWithoutListener(T t, String [] forNullColumns, Expression ... expressions);

    <P,R> boolean updatesLambdaNull(T t, LambdaQuery<P,R> [] forNullColumns,  Expression ... expressions);
    <P,R> boolean updatesLambdaNullWithoutListener(T t, LambdaQuery<P,R> [] forNullColumns, Expression ... expressions);

    boolean updates(T t, Condition condition);
    boolean updatesWithoutListener(T t, Condition condition);

    boolean updatesNull(T t, Condition condition);
    boolean updatesNullWithoutListener(T t, Condition condition);

    boolean updatesWithoutListener(Expression [] values, Expression [] conditions);

    boolean updatesNull(T t, String [] forNullColumns,  Condition condition);
    boolean updatesNullWithoutListener(T t, String [] forNullColumns, Condition condition);

    <P,R> boolean updatesLambdaNull(T t, LambdaQuery<P,R> [] forNullColumns,  Condition condition);
    <P,R> boolean updatesLambdaNullWithoutListener(T t, LambdaQuery<P,R> [] forNullColumns, Condition condition);

    /* 删除, (限制条件删除，危险的动作)
       testService.delete(test);
       删除（按主键)
       baseDao.delete(1, Test.class);
    */
    boolean delete(Object object);
    boolean deleteWithoutListener(Object object);

    boolean deletes(Condition condition);
    boolean deletesWithoutListener(Condition condition);

    boolean deletes(Expression... expressions);
    boolean deletesWithoutListener(Expression... expressions);

    //查询该表总数据
    int count(Expression ... expressions);
    boolean exists(Condition condition);
    boolean exists(Expression ... expressions);

    //简单的查询(单表）
    List<T> listNames(String ...names);
    List<T> list();
    List<T> list(int start, int size);
    List<T> list(Expression ... expressions);

    List<T> list(Condition condition);
    List<T> list(Condition condition, MultiOrder multiOrder);
    List<T> list(Condition condition, MultiOrder multiOrder, Integer limit);
    List<T> list(Names names, Condition condition);
    List<T> list(Names names, Condition condition, MultiOrder multiOrder);
    List<T> list(Names names, Condition condition, MultiOrder multiOrder, Integer limit);

    //复杂的查询（多表）, 可以返回Bean，ExtBean，Map类型
    <S> List<S> query(IQuery query);//queryDistinctString
    <S> List<S> query(IQuery q, int size);
    <S> List<S> query(IQuery q, int offset, int size);
    int queryCount(IQuery query);
    <S> Pageable<S> queryPage(IQuery query, Pageable page);

    String tableName() throws ORMException;
    void createOrUpdate() throws ORMException;
}
