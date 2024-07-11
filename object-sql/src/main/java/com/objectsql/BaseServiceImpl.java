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

import com.objectsql.annotation.RdId;
import com.objectsql.exception.ORMException;
import com.objectsql.exception.ORMSQLException;
import com.objectsql.listener.*;
import com.objectsql.query.QueryUtils;
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;
import com.objectsql.helper.SQLHelperCreator;
import com.objectsql.helper.SQLHelper;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;

import javax.sql.DataSource;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class BaseServiceImpl<T> extends SQLServiceImpl implements IBaseService<T>, BeanFactoryAware{

    public BaseServiceImpl() {
       init();
    }

    private void init(){
        Type[] ts = ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments();
        thisClass = (Class<T>) ts[0];
        serviceClass = this.getClass();
    }

    public BaseServiceImpl(ObjectSQLManager objectSQLManager){
        super(objectSQLManager);
        init();
    }

    public BaseServiceImpl(DataSource dataSource){
        super(new ObjectSQLManager(dataSource));
        init();
    }

//    public Class getBaseClass(){
//        return thisClass;
//    }

    private List<IORMListener> listeners = new ArrayList<IORMListener>();

    private List<IDefaultListener> defaultListeners = new ArrayList<IDefaultListener>();

    private List<IChangeListener> changeListeners = new ArrayList<IChangeListener>();

    private List<IChangedListener> changedListeners = new ArrayList<IChangedListener>();

    private List<IQueryListener> queryListeners = new ArrayList<IQueryListener>();

    public List<IORMListener> getListeners() {
        return listeners;
    }

    public void setListeners(List<IORMListener> listeners) {
        this.listeners.clear();
        if(listeners != null) {
            this.listeners.addAll(listeners);
        }
    }

    public List<IDefaultListener> getDefaultListeners() {
        return defaultListeners;
    }

    public void setDefaultListeners(List<IDefaultListener> defaultListeners) {
        this.defaultListeners.clear();
        if(defaultListeners != null) {
            this.defaultListeners.addAll(defaultListeners);
        }
    }

    public List<IChangeListener> getChangeListeners() {
        return changeListeners;
    }

    public void setChangeListeners(List<IChangeListener> changeListeners) {
        this.changeListeners.clear();
        if(changeListeners != null) {
            this.changeListeners.addAll(changeListeners);
        }
    }

    public List<IChangedListener> getChangedListeners() {
        return changedListeners;
    }

    public void setChangedListeners(List<IChangedListener> changedListeners) {
        this.changedListeners.clear();
        if(changedListeners != null) {
            this.changedListeners.addAll(changedListeners);
        }
    }

    public List<IQueryListener> getQueryListeners() {
        return queryListeners;
    }

    public void setQueryListeners(List<IQueryListener> queryListeners) {
        this.queryListeners.clear();
        if(queryListeners != null) {
            this.queryListeners.addAll(queryListeners);
        }
    }

    public void copyAllListeners(IBaseService service){
        if(service != null){
            service.setChangedListeners(getChangedListeners());
            service.setListeners(getListeners());
            service.setChangeListeners(getChangeListeners());
            service.setQueryListeners(getQueryListeners());
            service.setDefaultListeners(getDefaultListeners());
        }
    }

    public void addDefaultListener(IDefaultListener listener){
        if(!defaultListeners.contains(listener)) {
            defaultListeners.add(listener);
        }
    }

    public void removeDefaultListener(IDefaultListener listener){
        if(defaultListeners.contains(listener)) {
            defaultListeners.remove(listener);
        }
    }

    public void addChangeListener(IChangeListener listener){
        if(!changeListeners.contains(listener)){
            changeListeners.add(listener);
        }
    }

    public void removeChangeListener(IChangeListener listener){
        if(changeListeners.contains(listener)){
            changeListeners.remove(listener);
        }
    }

    public void addChangedListener(IChangedListener listener){
        if(!changedListeners.contains(listener)){
            changedListeners.add(listener);
        }
    }

    public void removeChangedListener(IChangedListener listener){
        if(changedListeners.contains(listener)){
            changedListeners.remove(listener);
        }
    }


    public void addORMListener(IORMListener listener){
        if(!listeners.contains(listener)){
            listeners.add(listener);
        }
    }
    public void removeORMListener(IORMListener listener){
        if(listeners.contains(listener)){
            listeners.remove(listener);
        }
    }

    public void addQueryListener(IQueryListener listener){
        if(!queryListeners.contains(listener)){
            queryListeners.add(listener);
        }
    }
    public void removeQueryListener(IQueryListener listener){
        if(queryListeners.contains(listener)){
            queryListeners.remove(listener);
        }
    }

    private <S> void sortAddListeners(List<S> result, Class<S> clazz){
        Map<String, S> listenerMap = BeanFactoryUtils.beansOfTypeIncludingAncestors(factory, clazz);
        for(S listener : listenerMap.values()){
            if(IBaseService.class.isAssignableFrom(listener.getClass())){
                continue;
            }
            if(ORMUtils.isTheSameClass(thisClass, listener.getClass())){
                result.add(listener);
            }
        }
    }


    private ListableBeanFactory factory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {

        this.factory = (ListableBeanFactory)beanFactory;

        sortAddListeners(listeners, IORMListener.class);
        sortAddListeners(defaultListeners, IDefaultListener.class);
        sortAddListeners(changeListeners, IChangeListener.class);
        sortAddListeners(changedListeners, IChangedListener.class);
        sortAddListeners(queryListeners, IQueryListener.class);
    }

    private void triggerQueryListener(IQuery query){
        for (IQueryListener listener : queryListeners) {
            listener.handle(thisClass, query);
        }
    }

    public void triggerDefaultListener(ORMType type, T  t){
        for (IDefaultListener listener : defaultListeners) {
            try {
                if(type == ORMType.INSERT) {
                    listener.insert(t);
                }else if(type == ORMType.UPDATE){
                    listener.update(t);
                }else if(type == ORMType.DELETE){
                    listener.delete(t);
                }else{
                    throw new ORMException("Other type, not support.");
                }
            }catch (Exception e){
                throw new ORMException("Trigger default listener error", e);
            }
        }
    }

    public void triggerORMListener(ORMType type, T t) {
        triggerORMListener(type, t, false, null);
    }

    public void triggerORMListener(ORMType type, T t, boolean updateNull, String[] nullColumns){
        for (IORMListener listener : listeners) {
            try {
                if(type == ORMType.INSERT) {
                    listener.insert(t);
                }else if(type == ORMType.UPDATE){
                    listener.update(t);
                    listener.update(t, updateNull, nullColumns);
                }else if(type == ORMType.DELETE){
                    listener.delete(t);
                }else{
                    throw new ORMException("Other type, not support.");
                }
            }catch (Exception e){
                throw new ORMException("Trigger orm listener error", e);
            }
        }
    }

    @Override
    public void changed(ORMType ormType, ORMOption option) {
        for (final IChangedListener listener : changedListeners) {
            try {
                if(listener.useDefault()) {
                    listener.changed(option.getOriginal(), option.getCurrent());
                }else{
                    listener.changed(ormType, option, option.getOriginal(), option.getCurrent());
                }
            }catch (Exception e){//变更异常不影响其他监听器。
                ORMUtils.handleDebugInfo(getClass(), "changed", ormType, option, e);
            }
        }
    }

    public void triggerChangeListener(ORMType ormType, final T original, final T t){
        triggerChangeListener(ormType, original, t, false, null);
    }

    public void triggerChangeListener(ORMType ormType, final T original, final T t, boolean updateNull, String [] nullColumns){
        for (final IChangeListener listener : changeListeners) {
            try {
                if(listener.useDefault()) {
                    listener.change(original, t);
                }else {
                    listener.change(ormType, new ORMOption(updateNull, nullColumns, original, t), original, t);
                }
            }catch (Exception e){
                if(e instanceof RuntimeException) {//事务，变更影响其他监听器
                    throw e;
                }else{
                    throw new ORMException(e);
                }
            }
        }
    }


    public void triggerChangedListenerImmediately(ORMType ormType, T original, T current) {
        changed(ormType, new ORMOption(false, null, original, current));
    }

    public void triggerChangedListenerImmediately(ORMType ormType, T original, T current, boolean updateNull, String[] nullColumns) {
        changed(ormType, new ORMOption(updateNull, nullColumns, original, current));
    }

    public void triggerChangedListenerWhenTransactionFinish(IServiceChangedListener serviceChangedListener, ORMType ormType, T original, T current) {
        triggerChangedListenerWhenTransactionFinish(serviceChangedListener, ormType, original, current, false, null);
    }

    public void triggerChangedListenerWhenTransactionFinish(IServiceChangedListener serviceChangedListener, ORMType ormType, T original, T current, boolean updateNull, String[] nullColumns) {
        if(!changedListeners.isEmpty()){//只保留最后一个操作对象
            ChangeHolder.cache(new PreChangeCache(serviceChangedListener, ormType, new ORMOption(updateNull, nullColumns, original, current)));
        }
    }

    public void beforeTriggerChangedListener(ORMType ormType, final T original, final T t, Connection connection) {
        beforeTriggerChangedListener(ormType, original, t, false, null, connection);
    }

    public void beforeTriggerChangedListener(ORMType ormType, final T original, final T t, boolean updateNull, String [] nullColumns, Connection connection){
        boolean autoCommit = true;
        try{
            autoCommit = connection.getAutoCommit();
        }catch (Exception e){
            e.printStackTrace();
        }
        if(autoCommit){
            changed(ormType, new ORMOption(updateNull, nullColumns, original, t));
            return;
        }
        triggerChangedListenerWhenTransactionFinish(this, ormType, original, t, updateNull, nullColumns);
    }

    public boolean insert(T t) {
        return insert(t, true);
    }

    public boolean insertWithoutListener(T t){
        return insert(t, false);
    }

    private boolean insert(T t, boolean enableListener) {
        PreparedStatement ps = null;
        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            if(enableListener) {
                triggerDefaultListener(ORMType.INSERT, t);
            }
            Options options = getOptions();
            helper = SQLHelperCreator.insert(t, options);

            ORMUtils.handleDebugInfo(serviceClass, "insert(object)", helper);

            RdId rdId = null;
            if(helper.getIdValue() == null && helper.getIdField() != null) {
                rdId = helper.getIdField().getAnnotation(RdId.class);
            }
            if(rdId != null && rdId.autoIncrement()) {
                ps = conn.prepareStatement(helper.getSql(), Statement.RETURN_GENERATED_KEYS);
            }else{
                ps = conn.prepareStatement(helper.getSql());
            }
            //ps = conn.prepareStatement(sql);
            //ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            //ps = conn.prepareStatement(sql, new String[]{idCols.getFirst()});
            SQLHelperCreator.setParameter(options, ps, helper.getParameters(), conn);
            boolean flag =  ps.executeUpdate() > 0;
            try {
                if(rdId != null && rdId.autoIncrement()) {
                    ResultSet seqRs = ps.getGeneratedKeys();
                    if(seqRs.next()) {
                        Object key = seqRs.getObject(1);
                        helper.setId(t, key);
                    }
                    seqRs.close();
                }
            }catch (Exception e){
                throw new ORMSQLException(e, "Insert, Get id error").put("object", t).put("enableListener", enableListener);
            }
            if(flag && enableListener) {
                triggerORMListener(ORMType.INSERT, t);
                triggerChangeListener(ORMType.INSERT, null, t);
                beforeTriggerChangedListener(ORMType.INSERT, null, t, conn);
            }
            return flag;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "Insert").put("object", t).put("enableListener", enableListener).put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "Insert").put("object", t).put("enableListener", enableListener).put("helper", helper);
            }
        } finally{
            closeConnection(null, ps, conn);
        }
    }


    public boolean update(T t) {
        return update(t, false, true, null);
    }

    public boolean updateWithoutListener(T t) {
        return update(t, false, false, null);
    }

    public boolean updateWithoutListener(T t, boolean updateNull) {
        return update(t, updateNull, false, null);
    }

    public boolean update(T t, boolean updateNull){
        return update(t, updateNull, true, null);
    }

    public boolean updateNull(T t, String ...forNullColumns){
        return update(t, false, true, forNullColumns);
    }
    public boolean updateNullWithoutListener(T t, String ...forNullColumns){
        return update(t, false, false, forNullColumns);
    }

    public <P,R> boolean updateLambdaNull(T t, LambdaQuery<P,R> ...lambdaQueries){
        return update(t, false, true, QueryUtils.getColumns(lambdaQueries));
    }
    public <P,R> boolean updateLambdaNullWithoutListener(T t, LambdaQuery<P,R>  ...lambdaQueries){
        return update(t, false, false, QueryUtils.getColumns(lambdaQueries));
    }

    private boolean update(T t, boolean updateNull, boolean enableListener, String [] nullColumns) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            if(enableListener) {
                triggerDefaultListener(ORMType.UPDATE, t);
            }
            helper = SQLHelperCreator.update(getOptions(), t, null, updateNull, nullColumns);
            T original = null;
            if(helper.getPair() != null && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                original = get(helper.getPair().getValue());
            }

            ORMUtils.handleDebugInfo(serviceClass, "update(object)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener) {
                triggerORMListener(ORMType.UPDATE, t, updateNull, nullColumns);
                triggerChangeListener(ORMType.UPDATE, original, t, updateNull, nullColumns);
                beforeTriggerChangedListener(ORMType.UPDATE, original, t, updateNull, nullColumns, conn);
            }
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "update")
                    .put("object", t)
                    .put("updateNull", updateNull)
                    .put("enableListener", enableListener)
                    .put("nullColumns", nullColumns)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "update")
                        .put("object", t)
                        .put("updateNull", updateNull)
                        .put("enableListener", enableListener)
                        .put("nullColumns", nullColumns)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(null, ps, conn);
        }
    }


    public boolean updates(T t, Expression... expressions){
        return updates(t, expressions, false, true, null);
    }
    public boolean updatesWithoutListener(T t, Expression ... expressions){
        return updates(t, expressions, false, false, null);
    }
    public  boolean updatesNull(T t, Expression ... expressions){
        return updates(t, expressions, true, true, null);
    }
    public boolean updatesNullWithoutListener(T t, Expression ... expressions){
        return updates(t, expressions, true, false, null);
    }
    public boolean updatesNull(T t, String [] forNullColumns,  Expression ... expressions){
        return updates(t, expressions, false, true, forNullColumns);
    }
    public boolean updatesNullWithoutListener(T t, String [] forNullColumns, Expression ... expressions){
        return updates(t, expressions, false, false, forNullColumns);
    }

    public <P,R> boolean updatesLambdaNull(T t, LambdaQuery<P,R> [] lambdaQueries,  Expression ... expressions){
        return updates(t, expressions, false, true, QueryUtils.getColumns(lambdaQueries));
    }
    public <P,R> boolean updatesLambdaNullWithoutListener(T t, LambdaQuery<P,R> [] lambdaQueries, Expression ... expressions){
        return updates(t, expressions, false, false, QueryUtils.getColumns(lambdaQueries));
    }

    private boolean updates(T t, Expression [] expressions, boolean updateNull, boolean enableListener, String [] forNullColumns) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.update(getOptions(), t, expressions, updateNull, forNullColumns);
            List<T> originals = null;
            if(helper.getIdValue() == null && helper.getIdField() != null && expressions != null && (expressions.length > 0) && (!changeListeners.isEmpty()||!changedListeners.isEmpty()||!defaultListeners.isEmpty())) {
                originals = list(expressions);
            }
            if(enableListener && originals != null){
                for(T row : originals){
                    triggerDefaultListener(ORMType.UPDATE, row);
                }
            }

            ORMUtils.handleDebugInfo(serviceClass, "updates(object)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener &&  (originals != null)) {
                for(T original : originals) {//updates重新注入id
                    Object idValue = ORMUtils.getFieldValue(original, helper.getIdField());
                    ORMUtils.setFieldValue(t, helper.getIdField(), idValue);
                    triggerORMListener(ORMType.UPDATE, t, updateNull, forNullColumns);
                    triggerChangeListener(ORMType.UPDATE, original, t, updateNull, forNullColumns);
                    beforeTriggerChangedListener(ORMType.UPDATE, original, t, updateNull, forNullColumns, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "update")
                    .put("object", t)
                    .put("updateNull", updateNull)
                    .put("enableListener", enableListener)
                    .put("expressions", expressions)
                    .put("nullColumns", forNullColumns)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "update")
                        .put("object", t)
                        .put("updateNull", updateNull)
                        .put("enableListener", enableListener)
                        .put("expressions", expressions)
                        .put("nullColumns", forNullColumns)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(null, ps, conn);
        }
    }

    public boolean updates(T t, Condition condition){
        return updates(t, condition, false, true, null);
    }

    public boolean updatesWithoutListener(T t, Condition condition){
        return updates(t, condition, false, false, null);
    }

    public  boolean updatesNull(T t, Condition condition){
        return updates(t, condition, true, true, null);
    }
    public  boolean updatesNullWithoutListener(T t, Condition condition){
        return updates(t, condition, true, false, null);
    }

    public boolean updatesNull(T t, String [] forNullColumns,  Condition condition){
        return updates(t, condition, false, true, forNullColumns);
    }
    public boolean updatesNullWithoutListener(T t, String [] forNullColumns, Condition condition){
        return updates(t, condition, false, false, forNullColumns);
    }

    public <P,R> boolean updatesLambdaNull(T t, LambdaQuery<P,R> [] lambdaQueries,  Condition condition){
        return updates(t, condition, false, true, QueryUtils.getColumns(lambdaQueries));
    }

    public <P,R> boolean updatesLambdaNullWithoutListener(T t, LambdaQuery<P,R> [] lambdaQueries, Condition condition){
        return updates(t, condition, false, false, QueryUtils.getColumns(lambdaQueries));
    }

    public boolean updatesWithoutListener(Expression [] values, Expression [] conditions){
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.updateExpress(thisClass, getOptions(), values, conditions);

            ORMUtils.handleDebugInfo(serviceClass, "updatesWithoutListener", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            boolean result = ps.executeUpdate() > 0;
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "updatesWithoutListener")
                    .put("values", values).put("expression", conditions).put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "updatesWithoutListener")
                        .put("values", values).put("expression", conditions).put("helper", helper);
            }
        } finally{
            closeConnection(null, ps, conn);
        }
    }
    private boolean updates(T t, Condition condition, boolean updateNull, boolean enableListener, String [] forNullColumns) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.updateTerms(getOptions(), t, condition, updateNull, forNullColumns);
            List<T> originals = null;
            if(helper.getIdValue() == null && helper.getIdField() != null && condition != null && (!changeListeners.isEmpty()||!changedListeners.isEmpty()||!defaultListeners.isEmpty())) {
                originals = list(condition);
            }
            if(enableListener && originals != null){
                for(T row : originals){
                    triggerDefaultListener(ORMType.UPDATE, row);
                }
            }

            ORMUtils.handleDebugInfo(serviceClass, "updates(object,condition)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener &&  (originals != null)) {
                for(T original : originals) {
                    Object idValue = ORMUtils.getFieldValue(original, helper.getIdField());
                    ORMUtils.setFieldValue(t, helper.getIdField(), idValue);
                    triggerORMListener(ORMType.UPDATE, t, updateNull, forNullColumns);
                    triggerChangeListener(ORMType.UPDATE, original, t, updateNull, forNullColumns);
                    beforeTriggerChangedListener(ORMType.UPDATE, original, t, updateNull, forNullColumns, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "updatesWithoutListener")
                    .put("object", t)
                    .put("condition", condition)
                    .put("updateNull", updateNull)
                    .put("enableListener", enableListener)
                    .put("nullColumns", forNullColumns)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "updatesWithoutListener")
                        .put("object", t)
                        .put("condition", condition)
                        .put("updateNull", updateNull)
                        .put("enableListener", enableListener)
                        .put("nullColumns", forNullColumns)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(null, ps, conn);
        }
    }


    public boolean delete(Object t) {
        return delete(t, true);
    }

    public boolean deleteWithoutListener(Object object){
        return delete(object, false);
    }

    private boolean delete(Object t, boolean enableListener) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            T now = null;
            conn = getConnection();
            if(thisClass.isInstance(t)){
                helper = SQLHelperCreator.delete(t);
                now = (T)t;
            }else{
                helper = SQLHelperCreator.delete(thisClass, t);
                if(enableListener && (helper.getPair() != null) &&
                        (!changeListeners.isEmpty()||!changedListeners.isEmpty()||!defaultListeners.isEmpty())) {
                    now = get(helper.getPair().getValue());
                }
            }

            ORMUtils.handleDebugInfo(serviceClass, "delete(object)", helper);

            if(enableListener) {
                triggerDefaultListener(ORMType.DELETE, now);
            }

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener) {
                triggerORMListener(ORMType.DELETE, now);
                triggerChangeListener(ORMType.DELETE, now, null);
                beforeTriggerChangedListener(ORMType.DELETE, now, null, conn);
            }
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "delete")
                    .put("object", t)
                    .put("enableListener", enableListener)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "delete")
                        .put("object", t)
                        .put("enableListener", enableListener)
                        .put("helper", helper);
            }
        }  finally{
            closeConnection(null, ps, conn);
        }
    }

    public boolean deletes(Condition condition){
        return deletes(condition, true);
    }

    public boolean deletesWithoutListener(Condition condition){
        return deletes(condition, false);
    }

    public boolean deletes(Expression ... expressions) {
        return deletes(true, expressions);
    }

    public boolean deletesWithoutListener(Expression ... expressions) {
        return deletes(false, expressions);
    }

    private boolean deletes(Condition condition, boolean enableListener) {
        if(condition == null){
            throw new ORMException("TABLE_DELETE_WITHOUT_EXPRESS, DELETE(Conditions) : " + serviceClass);
        }

        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            List<T> nows = null;
            conn = getConnection();
            helper = SQLHelperCreator.deleteBy(thisClass,getOptions(), condition);
            if(enableListener && (!changeListeners.isEmpty()||!changedListeners.isEmpty()||!defaultListeners.isEmpty())) {
                nows = list(condition);
                if(nows != null) {
                    for (T t : nows) {
                        triggerDefaultListener(ORMType.DELETE, t);
                    }
                }
            }

            ORMUtils.handleDebugInfo(serviceClass, "deletes(condition)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(),conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && nows != null) {
                for(T t : nows) {
                    triggerORMListener(ORMType.DELETE, t);
                    triggerChangeListener(ORMType.DELETE, t, null);
                    beforeTriggerChangedListener(ORMType.DELETE, t, null, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "delete")
                    .put("condition", condition)
                    .put("enableListener", enableListener)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "delete")
                        .put("condition", condition)
                        .put("enableListener", enableListener)
                        .put("helper", helper);
            }
        }  finally{
            closeConnection(null, ps, conn);
        }
    }

    private boolean deletes(boolean enableListener, Expression ... expressions) {

        if(expressions == null || expressions.length == 0){
            throw new ORMException("TABLE_DELETE_WITHOUT_EXPRESS, DELETE(Expressions) : " + serviceClass);
        }

        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            List<T> nows = null;
            conn = getConnection();
            helper = SQLHelperCreator.deleteBy(thisClass, getOptions(), expressions);
            if(enableListener && (!changeListeners.isEmpty()||!changedListeners.isEmpty()||!defaultListeners.isEmpty())) {
                nows = list(expressions);
                if(nows != null) {
                    for (T t : nows) {
                        triggerDefaultListener(ORMType.DELETE, t);
                    }
                }
            }

            ORMUtils.handleDebugInfo(serviceClass, "deletes(expressions)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(),conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && nows != null) {
                for(T t : nows) {
                    triggerORMListener(ORMType.DELETE, t);
                    triggerChangeListener(ORMType.DELETE, t, null);
                    beforeTriggerChangedListener(ORMType.DELETE, t, null, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            throw new ORMSQLException(e, "deletes")
                    .put("expressions", expressions)
                    .put("enableListener", enableListener)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw e;
            }else{
                throw new ORMSQLException(e, "deletes")
                        .put("expressions", expressions)
                        .put("enableListener", enableListener)
                        .put("helper", helper);
            }
        }  finally{
            closeConnection(null, ps, conn);
        }
    }

    public List<T> list() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = getOptions().doQuery(thisClass, null, null, null, null, null);

            ORMUtils.handleDebugInfo(serviceClass, "list", helper);

            ps = conn.prepareStatement(helper.getSql());
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "list")
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "deletes")
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public List<T> listNames(String ... names) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = getOptions().doQuery(thisClass, names, null, null, null, null);

            ORMUtils.handleDebugInfo(serviceClass, "listNames(names)", helper);

            ps = conn.prepareStatement(helper.getSql());
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs,getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "listNames")
                    .put("names", names)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "listNames")
                        .put("names", names)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public List<T> list(int start, int size) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = getOptions().doQuery(thisClass, null, null, null, start, size);

            ORMUtils.handleDebugInfo(serviceClass, "list(start,size)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "list")
                    .put("start", start)
                    .put("size", size)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "list")
                        .put("start", start)
                        .put("size", size)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public List<T> list(Expression ... expressions) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.query(getOptions(), thisClass, expressions);

            ORMUtils.handleDebugInfo(serviceClass, "list(expressions)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "list")
                    .put("expressions", expressions)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "list")
                        .put("expressions", expressions)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public List<T> list(Condition condition) {
        return list(null, condition, null, null);
    }


    @Override
    public List<T> list(Condition condition, MultiOrder multiOrder) {
        return list(null, condition, multiOrder, null);
    }


    @Override
    public List<T> list(Condition condition, MultiOrder multiOrder, Integer limit) {
        return list(null, condition, multiOrder, limit);
    }

    @Override
    public List<T> list(Names names, Condition condition) {
        return list(names, condition, null, null);
    }

    @Override
    public List<T> list(Names names, Condition condition, MultiOrder multiOrder) {
         return list(names, condition, multiOrder, null);
    }

    @Override
    public List<T> list(Names names, Condition condition, MultiOrder multiOrder, Integer limit) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = getOptions().doQuery(thisClass, names != null? names.names():null, condition, multiOrder, 0, limit);

            ORMUtils.handleDebugInfo(serviceClass, "list(names, condition, multiOrder, limit)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "list")
                    .put("names", names)
                    .put("condition", condition)
                    .put("order", multiOrder)
                    .put("limit", limit)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "list")
                        .put("names", names)
                        .put("condition", condition)
                        .put("order", multiOrder)
                        .put("limit", limit)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }


    public boolean exists(Condition condition) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.queryCount(getOptions(), this.thisClass, condition);

            ORMUtils.handleDebugInfo(serviceClass, "exists(condition)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object tmp = rs.getObject(1);
                if(tmp != null) {
                    return Integer.parseInt(tmp.toString()) > 0;
                }
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "exists")
                    .put("condition", condition)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "exists")
                        .put("condition", condition)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return false;
    }

    public boolean exists(Expression ... expressions) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.queryCountExpress(getOptions(), this.thisClass, expressions);

            ORMUtils.handleDebugInfo(serviceClass, "exists(expressions)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object tmp = rs.getObject(1);
                if(tmp != null) {
                    return Integer.parseInt(tmp.toString()) > 0;
                }
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "exists")
                    .put("expressions", expressions)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "exists")
                        .put("expressions", expressions)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return false;
    }

    public int count(Expression ... expressions){

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.queryCountExpress(getOptions(), this.thisClass, expressions);

            ORMUtils.handleDebugInfo(serviceClass, "count(expressions)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object tmp = rs.getObject(1);
                if(tmp != null) {
                    return Integer.parseInt(tmp.toString());
                }
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "count")
                    .put("expressions", expressions)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "count")
                        .put("expressions", expressions)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return 0;
    }

    private <S> S getQuery(IQuery query){
        S s = null;

        Connection conn = getConnection();
        query.setOptions(getOptions());

        triggerQueryListener(query);

        if (!query.dataPermission()){
            closeConnection(null, null, conn);
            return null;
        }

        //setClob通用
        QueryInfo qinfo = getOptions().doQuery(query, null);// = queryString(names, false);

        ORMUtils.handleDebugInfo(serviceClass, "getQuery(query)", qinfo);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            if(rs.next()){
                s = SQLHelperCreator.newClass(qinfo.getClazz(), rs, query.getQueryConvert(), getResultSetHandler());
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "getQuery")
                    .put("query", query)
                    .put("queryInfo", qinfo);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "getQuery")
                        .put("query", query)
                        .put("queryInfo", qinfo);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return s;
    }

    public <S> S get(Object object) {

        if(object == null){
            return null;
        }

        if(IQuery.class.isAssignableFrom(object.getClass())){
            return getQuery((IQuery)object);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        T temp = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            if(thisClass.isInstance(object)){
                helper = SQLHelperCreator.get(object);
            }else{
                helper = SQLHelperCreator.get(thisClass, object);
            }

            ORMUtils.handleDebugInfo(serviceClass, "get(object)", helper);

            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                temp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "get")
                    .put("object", object)
                    .put("helper", helper);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "getQuery")
                        .put("object", object)
                        .put("helper", helper);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return (S)temp;
    }


    public <S> List<S> query(IQuery q){
        return innerQuery(q);
    }

    public <S> List<S> query(IQuery q, int size){
        Pageable page = new Pageable();
        page.setPage(1);
        page.setSize(size);
        q.setPageable(page);
        return innerQuery(q);
    }

    public <S> List<S> query(IQuery q, int offset, int size){
        Pageable page = new Pageable();
        page.setSize(size);
        page.setOffset(offset);
        q.setPageable(page);
        return innerQuery(q);
    }


    private <S> List<S> innerQuery(IQuery query){

        //setClob通用
        List<S> temp = new ArrayList<S>();

        Connection conn = getConnection();

        query.setOptions(getOptions());

        triggerQueryListener(query);

        if (!query.dataPermission()){
            closeConnection(null, null, conn);
            return temp;
        }

        QueryInfo qinfo =  query.doQuery(); //getQueryPage().doQuery(q, page);// = queryString(names, false);

        ORMUtils.handleDebugInfo(serviceClass, "innerQuery(query)", qinfo);

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            while(rs.next()){
                S t = SQLHelperCreator.newClass(qinfo.getClazz(), rs, query.getQueryConvert(), getResultSetHandler());
                temp.add(t);
            }
        } catch (SQLException e) {
            throw new ORMSQLException(e, "innerQuery")
                    .put("query", query)
                    .put("queryInfo", qinfo);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "innerQuery")
                        .put("query", query)
                        .put("queryInfo", qinfo);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public int queryCount(IQuery q){
        //setClob通用
        int count = 0;

        Connection conn = null;

        PreparedStatement ps = null;
        ResultSet rs = null;
        QueryInfo qinfo = null;
        try {
            conn = getConnection();
            q.setOptions(getOptions());
            triggerQueryListener(q);
            if (!q.dataPermission()){
                closeConnection(null, null, conn);
                return count;
            }
            qinfo = q.doQueryCount();

            ORMUtils.handleDebugInfo(serviceClass, "queryCount(query)", qinfo);

            String query = qinfo.getSql();// = queryString(names, false);

            ps = conn.prepareStatement(query);
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            if(rs.next()){
                count = rs.getInt(1);
            }

        } catch (SQLException e) {
            throw new ORMSQLException(e, "queryCount")
                    .put("query", q)
                    .put("queryInfo", qinfo);
        } catch (Exception e) {
            if (e instanceof ORMException){
                throw (ORMException)e;
            }else{
                throw new ORMSQLException(e, "queryCount")
                        .put("query", q)
                        .put("queryInfo", qinfo);
            }
        } finally{
            closeConnection(rs, ps, conn);
        }
        return count;
    }

    public <S> Pageable<S> queryPage(IQuery query, Pageable page){
        if(page == null){
            page = new Pageable();
        }
        query.setPageable(page);
        //setClob通用
        List<S> temp = new ArrayList<S>();

        Connection conn = getConnection();

        query.setOptions(getOptions());

        triggerQueryListener(query);

        if (!query.dataPermission()){
            page.setRows(temp);
            page.setTotal(temp.size());
            closeConnection(null, null, conn);
            return page;
        }

        QueryInfo qinfo =  null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        int total = 0;
        try{
            try {
                qinfo = query.doQueryCount();

                ORMUtils.handleDebugInfo(serviceClass, "queryPage(query, page)", qinfo);

                String countSQL = qinfo.getSql();// = queryString(names, false);

                ps = conn.prepareStatement(countSQL);
                SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
                rs = ps.executeQuery();

                if(rs.next()){
                    total = rs.getInt(1);
                }
            } catch (SQLException e) {
                throw new ORMSQLException(e, "queryPage")
                        .put("type", "count")
                        .put("query", query)
                        .put("page", page)
                        .put("queryInfo", qinfo);
            } catch (Exception e) {
                if (e instanceof ORMException) {
                    throw (ORMException) e;
                } else {
                    throw new ORMSQLException(e, "queryPage")
                            .put("type", "count")
                            .put("query", query)
                            .put("page", page)
                            .put("queryInfo", qinfo);
                }
            } finally {
                closeConnection(rs, ps, null);
            }
            if(total > 0) {
                qinfo = query.doQuery(); //getQueryPage().doQuery(q, page);// = queryString(names, false);

                ORMUtils.handleDebugInfo(serviceClass, "queryPage(query, page)", qinfo);

                try {
                    ps = conn.prepareStatement(qinfo.getSql());
                    SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
                    rs = ps.executeQuery();
                    while (rs.next()) {
                        S t = SQLHelperCreator.newClass(qinfo.getClazz(), rs, query.getQueryConvert(), getResultSetHandler());
                        temp.add(t);
                    }
                } catch (SQLException e) {
                    throw new ORMSQLException(e, "queryPage")
                            .put("type", "data")
                            .put("query", query)
                            .put("page", page)
                            .put("queryInfo", qinfo);
                } catch (Exception e) {
                    if (e instanceof ORMException) {
                        throw (ORMException) e;
                    } else {
                        throw new ORMSQLException(e, "queryPage")
                                .put("type", "data")
                                .put("query", query)
                                .put("page", page)
                                .put("queryInfo", qinfo);
                    }
                } finally {
                    closeConnection(rs, ps, null);
                }
            }
        } finally {
            closeConnection(null, null, conn);
        }
        page.setRows(temp);
        page.setTotal(total);
        return page;
    }

    @Override
    public String tableName() throws ORMException{
        return super.getTableName(thisClass);
    }

    @Override
    public void createOrUpdate() throws ORMException{
        super.createOrUpdate(thisClass);
    }
}
