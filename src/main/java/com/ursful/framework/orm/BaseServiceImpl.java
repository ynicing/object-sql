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

import com.ursful.framework.orm.annotation.RdId;
import com.ursful.framework.orm.exception.ORMException;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.listener.IChangeListener;
import com.ursful.framework.orm.listener.IChangedListener;
import com.ursful.framework.orm.listener.IDefaultListener;
import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.orm.helper.SQLHelperCreator;
import com.ursful.framework.orm.listener.IORMListener;
import com.ursful.framework.orm.helper.SQLHelper;
import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.BeanFactoryUtils;
import org.springframework.beans.factory.ListableBeanFactory;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class BaseServiceImpl<T> extends SQLServiceImpl implements IBaseService<T>, BeanFactoryAware{

    private Logger logger = Logger.getLogger(BaseServiceImpl.class);

    protected BaseServiceImpl() {
        Type[] ts = ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments();
        thisClass = (Class<T>) ts[0];
        serviceClass = this.getClass();
    }

//    public Class getBaseClass(){
//        return thisClass;
//    }

    private List<IORMListener> listeners = new ArrayList<IORMListener>();

    private List<IDefaultListener> defaultListeners = new ArrayList<IDefaultListener>();

    private List<IChangeListener> changeListeners = new ArrayList<IChangeListener>();

    private List<IChangedListener> changedListeners = new ArrayList<IChangedListener>();

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
    }


    private void triggerDefaultListener(T  t, boolean isUpdate){
        for (IDefaultListener listener : defaultListeners) {
            try {
                if(isUpdate) {
                    listener.update(t);
                }else{
                    listener.insert(t);
                }
            }catch (Exception e){
                logger.error("Default Listener Error", e);
                throw e;
            }
        }
    }

    private void triggerORMListener(T t, ORMType type){
        for (IORMListener listener : listeners) {
            try {
                if(type == ORMType.INSERT) {
                    listener.insert(t);
                }else if(type == ORMType.UPDATE){
                    listener.update(t);
                }else if(type == ORMType.DELETE){
                    listener.delete(t);
                }else{
                    logger.warn("Other type, not support.");
                }
            }catch (Exception e){
                logger.error("ORM Listener Error", e);
                throw e;
            }
        }
    }

    @Override
    public void changed(T original, T current) {
        for (final IChangedListener listener : changedListeners) {
            try {
                listener.changed(original, current);
            }catch (Exception e){//变更异常不影响其他监听器。
                logger.error("Changed Listener Error", e);
            }
        }
    }

    private void triggerChangeListener(final T original, final T t, Connection connection){
        boolean autoCommit = true;
        try{
            autoCommit = connection.getAutoCommit();
        }catch (Exception e){
            e.printStackTrace();
        }
        for (final IChangeListener listener : changeListeners) {
            try {
                listener.change(original, t);
            }catch (Exception e){
                logger.error("Change Listener Error", e);
                if(!autoCommit || (e instanceof RuntimeException)) {//事务，变更影响其他监听器
                    throw e;
                }
            }
        }
        if(autoCommit){
            changed(original, t);
            return;
        }
        if(!changedListeners.isEmpty()){
            ChangeHolder.cache(new PreChangeCache(this, original, t));
        }

    }

    public boolean save(T t) {
        return save(t, true);
    }

    public boolean saveWithoutListener(T t){
        return save(t, false);
    }

    private boolean save(T t, boolean enableListener) {
        PreparedStatement ps = null;
        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            if(enableListener) {
                triggerDefaultListener(t, false);
            }
            Options options = getOptions();
            helper = SQLHelperCreator.insert(t, options);
            if(ORMUtils.getDebug()) {
                logger.info("SAVE : " + helper);
            }
            logger.debug("connection :" + conn);
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
                logger.error("not support.", e);
            }
            if(flag && enableListener) {
                triggerORMListener(t, ORMType.INSERT);
                triggerChangeListener(null, t, conn);
            }
            return flag;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, SAVE: " + e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, SAVE Listener : " + e.getMessage());
        } finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
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

    private boolean update(T t, boolean updateNull, boolean enableListener, String [] nullColumns) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            if(enableListener) {
                triggerDefaultListener(t, true);
            }
            helper = SQLHelperCreator.update(getOptions(), t, null, updateNull, nullColumns);
            T original = null;
            if(helper.getPair() != null && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                original = get(helper.getPair().getValue());
            }
            if(ORMUtils.getDebug()) {
                logger.info("UPDATE : " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener) {
                triggerORMListener(t, ORMType.UPDATE);
                triggerChangeListener(original, t, conn);
            }
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, UPDATE: " + e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, UPDATE Listener : " + e.getMessage());
        } finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
        }
    }


    public boolean updates(T t, Express... expresses){
        return updates(t, expresses, false, true, null);
    }
    public boolean updatesWithoutListener(T t, Express ... expresses){
        return updates(t, expresses, false, false, null);
    }
    public  boolean updatesNull(T t, Express ... expresses){
        return updates(t, expresses, true, true, null);
    }
    public boolean updatesNullWithoutListener(T t, Express ... expresses){
        return updates(t, expresses, true, false, null);
    }
    public boolean updatesNull(T t, String [] forNullColumns,  Express ... expresses){
        return updates(t, expresses, true, true, forNullColumns);
    }
    public boolean updatesNullWithoutListener(T t, String [] forNullColumns, Express ... expresses){
        return updates(t, expresses, true, false, forNullColumns);
    }

    private boolean updates(T t, Express [] expresses, boolean updateNull, boolean enableListener, String [] forNullColumns) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            //triggerDefaultListener(t, true);
            conn = getConnection();
            helper = SQLHelperCreator.update(getOptions(), t, expresses, updateNull, forNullColumns);
            List<T> originals = null;
            if(helper.getIdValue() == null && helper.getIdField() != null && expresses != null && (expresses.length > 0) && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                originals = list(expresses);
            }
            if(ORMUtils.getDebug()) {
                logger.info("UPDATE : " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener &&  (originals != null)) {
                for(T original : originals) {//updates重新注入id
                    Object idValue = ORMUtils.getFieldValue(original, helper.getIdField());
                    ORMUtils.setFieldValue(t, helper.getIdField(), idValue);
                    triggerORMListener(t, ORMType.UPDATE);
                    triggerChangeListener(original, t, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, UPDATE: " +  e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, UPDATE Listener : " + e.getMessage());
        } finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
        }
    }

    public boolean updates(T t, Terms terms){
        return updates(t, terms, false, true, null);
    }

    public boolean updatesWithoutListener(T t, Terms terms){
        return updates(t, terms, false, false, null);
    }

    public  boolean updatesNull(T t, Terms terms){
        return updates(t, terms, true, true, null);
    }
    public  boolean updatesNullWithoutListener(T t, Terms terms){
        return updates(t, terms, true, false, null);
    }

    public boolean updatesNull(T t, String [] forNullColumns,  Terms terms){
        return updates(t, terms, false, true, forNullColumns);
    }
    public boolean updatesNullWithoutListener(T t, String [] forNullColumns, Terms terms){
        return updates(t, terms, false, false, forNullColumns);
    }


    public boolean updatesWithoutListener(Express [] values, Express [] conditions){
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            //triggerDefaultListener(t, true);
            conn = getConnection();
            helper = SQLHelperCreator.updateExpress(thisClass, getOptions(), values, conditions);
            if(ORMUtils.getDebug()) {
                logger.info("UPDATE : " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            boolean result = ps.executeUpdate() > 0;
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("updatesWithoutListener, UPDATES: " +  e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("updatesWithoutListener, UPDATES Listener : " + e.getMessage());
        } finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
        }
    }

    private boolean updates(T t, Terms terms, boolean updateNull, boolean enableListener, String [] forNullColumns) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            //triggerDefaultListener(t, true);
            conn = getConnection();
            helper = SQLHelperCreator.updateTerms(getOptions(), t, terms, updateNull, forNullColumns);
            List<T> originals = null;
            if(helper.getIdValue() == null && helper.getIdField() != null && terms != null && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                originals = list(terms);
            }
            if(ORMUtils.getDebug()) {
                logger.info("UPDATE : " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener &&  (originals != null)) {
                for(T original : originals) {
                    Object idValue = ORMUtils.getFieldValue(original, helper.getIdField());
                    ORMUtils.setFieldValue(t, helper.getIdField(), idValue);
                    triggerORMListener(t, ORMType.UPDATE);
                    triggerChangeListener(original, t, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, UPDATE: " +  e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, UPDATE Listener : " + e.getMessage());
        } finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
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
                if(enableListener && (helper.getPair() != null) && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                    now = get(helper.getPair().getValue());
                }
            }
            if(ORMUtils.getDebug()) {
                logger.info("DELETE : " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener) {
                triggerORMListener(now, ORMType.DELETE);
                triggerChangeListener(now, null, conn);
            }
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, DELETE : " +  e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, DELETE : " + e.getMessage());
        }  finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
        }
    }

    public boolean deletes(Terms terms){
        return deletes(true, terms);
    }

    public boolean deletesWithoutListener(Terms terms){
        return deletes(false, terms);
    }

    public boolean deletes(Express ... expresses) {
        return deletes(true, expresses);
    }

    public boolean deletesWithoutListener(Express ... expresses) {
        return deletes(false, expresses);
    }

    private boolean deletes(boolean enableListener, Terms terms) {
        if(terms == null){
            throw new RuntimeException("TABLE_DELETE_WITHOUT_EXPRESS, DELETE(Terms) : " + thisClass);
        }

        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            List<T> nows = null;
            conn = getConnection();
            helper = SQLHelperCreator.deleteBy(thisClass,getOptions(), terms);
            if(enableListener && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                nows = list(terms);
            }
            if(ORMUtils.getDebug()) {
                logger.info("DELETE(Express): " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(),conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && nows != null) {
                for(T t : nows) {
                    triggerORMListener(t, ORMType.DELETE);
                    triggerChangeListener(t, null, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, DELETE(Terms) : " +  e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, DELETE(Terms) : " + e.getMessage());
        }  finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
        }
    }

    private boolean deletes(boolean enableListener, Express ... expresses) {

        if(expresses == null || expresses.length == 0){
            throw new RuntimeException("TABLE_DELETE_WITHOUT_EXPRESS, DELETE(Express) : " + thisClass);
        }

        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            List<T> nows = null;
            conn = getConnection();
            helper = SQLHelperCreator.deleteBy(thisClass, getOptions(), expresses);
            if(enableListener && (!changeListeners.isEmpty()||!changedListeners.isEmpty())) {
                nows = list(expresses);
            }
            if(ORMUtils.getDebug()) {
                logger.info("DELETE(Express): " + helper);
            }
            logger.debug("connection :" + conn);
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(),conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && nows != null) {
                for(T t : nows) {
                    triggerORMListener(t, ORMType.DELETE);
                    triggerChangeListener(t, null, conn);
                }
            }
            return result;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, DELETE(Express) : " +  e.getMessage());
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, DELETE(Express) : " + e.getMessage());
        }  finally{
            closeConnection(null, ps, conn);
            logger.debug("close connection :" + conn);
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
            if(ORMUtils.getDebug()) {
                logger.info("list() : " + helper);
            }
            ps = conn.prepareStatement(helper.getSql());
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, LIST: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, LIST: " +  e.getMessage());
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
            if(ORMUtils.getDebug()) {
                logger.info("list() : " + helper);
            }
            ps = conn.prepareStatement(helper.getSql());
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs,getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, LIST: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, LIST: " +  e.getMessage());
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
            if(ORMUtils.getDebug()) {
                logger.info("list(start,size) : " + helper);
            }
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, LIST(Start,Size): " + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, List(Start,Size): " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public List<T> list(Express ... expresses) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.query(getOptions(), thisClass, expresses);
            if(ORMUtils.getDebug()) {
                logger.info("list(expresses) : " + helper);
            }
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, ListExpress: " + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, ListExpress: " + e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    public List<T> list(Terms terms) {
        return list(null, terms, null, null);
    }

    @Override
    public List<T> list(Terms terms, MultiOrder multiOrder) {
        return list(null, terms, multiOrder, null);
    }

    @Override
    public List<T> list(Terms terms, MultiOrder multiOrder, Integer limit) {
        return list(null, terms, multiOrder, limit);
    }

    @Override
    public List<T> list(Names names, Terms terms) {
        return list(names, terms, null, null);
    }

    @Override
    public List<T> list(Names names, Terms terms, MultiOrder multiOrder) {
         return list(names, terms, multiOrder, null);
    }

    @Override
    public List<T> list(Names names, Terms terms, MultiOrder multiOrder, Integer limit) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = getOptions().doQuery(thisClass, names != null? names.names():null, terms, multiOrder, null, limit);
            if(ORMUtils.getDebug()) {
                logger.info("list(names, terms, multiOrder, limit) : " + helper);
            }
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
                temp.add(tmp);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, ListTerm: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, ListTerm: " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }


    public boolean exists(Terms terms) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.queryCount(getOptions(), this.thisClass, terms);
            if(ORMUtils.getDebug()) {
                logger.info("exists : " + helper);
            }
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
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, Exists: " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return false;
    }

    public boolean exists(Express ... expresses) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.queryCountExpress(getOptions(), this.thisClass, expresses);
            if(ORMUtils.getDebug()) {
                logger.info("exists : " + helper);
            }
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
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, Exists: " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return false;
    }

    public int count(Express ... expresses){

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            helper = SQLHelperCreator.queryCountExpress(getOptions(), this.thisClass, expresses);
            if(ORMUtils.getDebug()) {
                logger.info("Count : " + helper);
            }
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
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, COUNT : " + e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return 0;
    }


    private <S> S getQuery(IQuery query){
        S s = null;

        Connection conn = getConnection();
        query.setOptions(getOptions());
        //setClob通用
        QueryInfo qinfo = getOptions().doQuery(query, null);// = queryString(names, false);

        if(ORMUtils.getDebug()) {
            logger.info("list:" + qinfo);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            if(rs.next()){
                s = SQLHelperCreator.newClass(qinfo.getClazz(), rs, getResultSetHandler());
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ERROR, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_SECURITY, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ARGUMENT, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return s;
    }


    public <S> S get(Object t) {

        if(t == null){
            return null;
        }

        if(IQuery.class.isAssignableFrom(t.getClass())){
            return getQuery((IQuery)t);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        T temp = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            conn = getConnection();
            if(thisClass.isInstance(t)){
                helper = SQLHelperCreator.get(t);
            }else{
                helper = SQLHelperCreator.get(thisClass, t);
            }
            if(ORMUtils.getDebug()) {
                logger.info("GET : " + helper);
            }
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, helper.getParameters(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                temp = SQLHelperCreator.newClass(thisClass, rs, getResultSetHandler());
            }
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ERROR, GET: " + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, GET: " +  e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return (S)temp;
    }


    public <S> List<S> query(IQuery q){
        return query(q, null);
    }

    public <S> List<S> query(IQuery q, int size){

        Pageable page = new Pageable();
        page.setPage(1);
        page.setSize(size);

        Connection conn = getConnection();
        //setClob通用

        List<S> temp = new ArrayList<S>();

        q.setOptions(getOptions());
        q.setPageable(page);
        QueryInfo qinfo =  q.doQuery();

//        QueryInfo qinfo = getQueryPage().doQuery(q, page);// = queryString(names, false);

        if(ORMUtils.getDebug()) {
            logger.info("list:" + qinfo);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            while(rs.next()){
                S t = SQLHelperCreator.newClass(qinfo.getClazz(), rs, getResultSetHandler());
                temp.add(t);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ERROR, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_SECURITY, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ARGUMENT, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }

    private <S> List<S> query(IQuery q, Pageable page){

        Connection conn = getConnection();
        //setClob通用


        List<S> temp = new ArrayList<S>();

        q.setOptions(getOptions());
        q.setPageable(page);

        QueryInfo qinfo =  q.doQuery(); //getQueryPage().doQuery(q, page);// = queryString(names, false);

        if(ORMUtils.getDebug()) {
            logger.info("list:" + qinfo);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            while(rs.next()){
                S t = SQLHelperCreator.newClass(qinfo.getClazz(), rs, getResultSetHandler());
                temp.add(t);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ERROR, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_SECURITY, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ACCESS, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ARGUMENT, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return temp;
    }


    public int queryCount(IQuery q){
        //setClob通用

        Connection conn = null;
        int count = 0;


        PreparedStatement ps = null;
        ResultSet rs = null;
        QueryInfo qinfo = null;
        try {
            conn = getConnection();
            q.setOptions(getOptions());
            qinfo = q.doQueryCount();
            if(ORMUtils.getDebug()) {
                logger.info("list:" + qinfo);
            }

            String query = qinfo.getSql();// = queryString(names, false);

            ps = conn.prepareStatement(query);
            SQLHelperCreator.setParameter(getOptions(), ps, qinfo.getValues(), conn);
            rs = ps.executeQuery();

            if(rs.next()){
                count = rs.getInt(1);
            }

        } catch (SQLException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ERROR, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_SECURITY, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new RuntimeException("QUERY_SQL_ILLEGAL_ARGUMENT, SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            closeConnection(rs, ps, conn);
        }
        return count;
    }


    public <S> Pageable<S> queryPage(IQuery query, Pageable page){
        if(page == null){
            page = new Pageable();
        }
        List<S> list = query(query, page);
        int total = queryCount(query);
        page.setRows(list);
        page.setTotal(total);
        return page;
    }

    /*
    public int executeBatch(ISQLScript script, Object[] ... parameters){
        ResultSet rs = null;
        PreparedStatement ps = null;

        Connection conn = null;
        try {
            //LogUtil.info("SQL:" + sql + " Parameers: " + parameters, BaseSQLImpl.class);
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            for(Object[] objects : parameters) {
                SQLHelperCreator.setParameter(getOptions(), ps, objects);
                ps.addBatch();
            }
            ps.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            close(conn, ps, rs);
        }
        return 0;
    }*/

    @Override
    public String tableName() throws ORMException{
        return super.getTableName(thisClass);
    }

    @Override
    public void createOrUpdate() throws ORMException{
        super.createOrUpdate(thisClass);
    }
}
