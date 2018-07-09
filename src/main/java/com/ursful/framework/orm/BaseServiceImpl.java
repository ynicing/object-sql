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
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;
import com.ursful.framework.orm.error.ORMErrorCode;
import com.ursful.framework.orm.helper.SQLHelperCreator;
import com.ursful.framework.orm.listener.IORMListener;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.helper.SQLHelper;
import com.ursful.framework.core.exception.CommonException;
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

    private static final int ERROR_QUERY_TABLE = 101;

    private Class<?> thisClass;

    protected BaseServiceImpl() {
        Type[] ts = ((ParameterizedType) getClass()
                .getGenericSuperclass()).getActualTypeArguments();
        thisClass = (Class<T>) ts[0];
    }

    private List<IORMListener> listeners = new ArrayList<IORMListener>();

    private List<IDefaultListener> defaultListeners = new ArrayList<IDefaultListener>();

    private List<IChangeListener> changeListeners = new ArrayList<IChangeListener>();

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

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {

        Map<String, IORMListener> listenerMap = BeanFactoryUtils.beansOfTypeIncludingAncestors((ListableBeanFactory)beanFactory, IORMListener.class);
        for(IORMListener listener : listenerMap.values()){
            if(ORMUtils.isTheSameClass(thisClass, listener.getClass())){
                listeners.add(listener);
            }
        }

        Map<String, IDefaultListener> map = BeanFactoryUtils.beansOfTypeIncludingAncestors((ListableBeanFactory)beanFactory, IDefaultListener.class);
        for(IDefaultListener listener : map.values()){
            if(ORMUtils.isTheSameClass(thisClass, listener.getClass())) {
                defaultListeners.add(listener);
            }
        }

        Map<String, IChangeListener> changes = BeanFactoryUtils.beansOfTypeIncludingAncestors((ListableBeanFactory)beanFactory, IChangeListener.class);
        for(IChangeListener listener : changes.values()){
            if(ORMUtils.isTheSameClass(thisClass, listener.getClass())){
                changeListeners.add(listener);
            }
        }
    }



    public boolean save(T t) {
        PreparedStatement ps = null;
        Connection conn = null;
        SQLHelper helper = null;
        try {

            for (IDefaultListener listener : defaultListeners) {
                try {
                    listener.insert(t);
                }catch (Exception e){
                    logger.error("Default Listener insert Error", e);
                }
            }

            helper = SQLHelperCreator.save(t, dataSourceManager.getDatabaseType());

            if(ORMUtils.getDebug()) {
                logger.info("SAVE : " + helper);
            }

            conn = getConnection();
            logger.debug("connection :" + conn);
            if(helper.getIdField() != null) {
                ps = conn.prepareStatement(helper.getSql(), Statement.RETURN_GENERATED_KEYS);
            }else{
                ps = conn.prepareStatement(helper.getSql());
            }
            //ps = conn.prepareStatement(sql);
            //ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            //ps = conn.prepareStatement(sql, new String[]{idCols.getFirst()});

            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            boolean flag =  ps.executeUpdate() > 0;

            try {
                if(helper.getIdField() != null) {
                    ResultSet seqRs = ps.getGeneratedKeys();
                    seqRs.next();
                    Object key = seqRs.getObject(1);
                    helper.setId(t, key);
                    seqRs.close();
                }
            }catch (Exception e){
                logger.error("not support.", e);
            }
            if(flag) {
                for (IORMListener listener : listeners) {
                    try {
                        listener.insert(t);
                    }catch (Exception e){
                        logger.error("ORM Listener insert Error", e);
                    }
                }

                for (IChangeListener listener : changeListeners) {
                    try {
                        listener.change(null, t);
                    }catch (Exception e){
                        logger.error("Change Listener Error", e);
                    }

                }
            }
            return flag;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "SAVE: " + e.getMessage());
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } finally{
            close(null, ps, conn);
        }
    }


    public boolean update(T t) {
        return update(t, false, true);
    }

    public boolean updateWithoutListener(T t) {
        return update(t, false, false);
    }

    public boolean updateWithoutListener(T t, boolean updateNull) {
        return update(t, updateNull, false);
    }

    public boolean update(T t, boolean updateNull){
        return update(t, updateNull, true);
    }

    private boolean update(T t, boolean updateNull, boolean enableListener) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {

            //if(!updateNull) {
            for (IDefaultListener listener : defaultListeners) {
                try {
                    listener.update(t);
                }catch (Exception e){
                    logger.error("Default Listener update Error", e);
                }
            }
            //}

            helper = SQLHelperCreator.update(t, dataSourceManager.getDatabaseType(), updateNull);
            Object original = null;
            if(helper.getPair() != null && !changeListeners.isEmpty()) {
                original = get(helper.getPair().getValue());
            }
            if(ORMUtils.getDebug()) {
                logger.info("UPDATE : " + helper);
            }

            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result && enableListener) {
                for (IORMListener listener : listeners) {
                    try {
                        listener.update(t);
                    }catch (Exception e){
                        logger.error("ORM Listener insert Error", e);
                    }
                }
                for (IChangeListener listener : changeListeners) {
                    try {
                        listener.change(original, t);
                    }catch (Exception e){
                        logger.error("Change Listener Error", e);
                    }
                }
            }
            return result;
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "UPDATE: " +  e.getMessage());
        } finally{
            close(null, ps, conn);
        }
    }


    public boolean delete(Object t) {
        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            Object now = null;

            if(thisClass.isInstance(t)){
                helper = SQLHelperCreator.delete(t);
                now = t;
            }else{
                helper = SQLHelperCreator.delete(thisClass, t);
                if(helper.getPair() != null && !changeListeners.isEmpty()) {
                    now = get(helper.getPair().getValue());
                }
            }
            if(ORMUtils.getDebug()) {
                logger.info("DELETE : " + helper);
            }

            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result) {
                for (IORMListener listener : listeners) {
                    try {
                        listener.delete(t);
                    }catch (Exception e){
                        logger.error("ORM Listener delete Error", e);
                    }

                }
                for (IChangeListener listener : changeListeners) {
                    try {
                        listener.change(now, null);
                    }catch (Exception e){
                        logger.error("Change Listener Error", e);
                    }
                }
            }
            return result;
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "DELETE : " +  e.getMessage());
        } finally{
            close(null, ps, conn);
        }
    }


    public boolean deletes(Express ... expresses) {

        if(expresses == null || expresses.length == 0){
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE,ORMErrorCode.TABLE_DELETE_WITHOUT_EXPRESS, "DELETE(Express) : " + thisClass);
        }

        PreparedStatement ps = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            List<T> nows = null;

            helper = SQLHelperCreator.deleteBy(thisClass, expresses);
            if(!changeListeners.isEmpty()) {
                nows = list(expresses);
            }
            if(ORMUtils.getDebug()) {
                logger.info("DELETE(Express): " + helper);
            }

            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);

            boolean result = ps.executeUpdate() > 0;
            if(result) {
                for (IORMListener listener : listeners) {
                    try {
                        for(T t : nows) {
                            listener.delete(t);
                        }
                    }catch (Exception e){
                        logger.error("ORM Listener delete(Express) Error", e);
                    }

                }
                for (IChangeListener listener : changeListeners) {
                    try {
                        for(T t : nows) {
                            listener.change(t, null);
                        }
                    }catch (Exception e){
                        logger.error("Change Listener delete(Express) Error", e);
                    }
                }
            }
            return result;
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "DELETE(Express) : " +  e.getMessage());
        } finally{
            close(null, ps, conn);
        }
    }

    public List<T> list() {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            helper = dataSourceManager.getQueryPage().doQuery(thisClass, null, null, null, null, null);
            if(ORMUtils.getDebug()) {
                logger.info("list() : " + helper);
            }

            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs);
                temp.add(tmp);
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "LIST: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "LIST: " +  e.getMessage());
        } finally{
            close(rs, ps, conn);
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
            helper = dataSourceManager.getQueryPage().doQuery(thisClass, null, null, null, start, size);
            if(ORMUtils.getDebug()) {
                logger.info("list(start,size) : " + helper);
            }
            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
           SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs);
                temp.add(tmp);
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "LIST(Start,Size): " + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "List(Start,Size): " +  e.getMessage());
        } finally{
            close(rs, ps, conn);
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
            helper = SQLHelperCreator.query(thisClass, expresses);

            if(ORMUtils.getDebug()) {
                logger.info("list(expresses) : " + helper);
            }

            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
           SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs);
                temp.add(tmp);
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "ListExpress: " + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "ListExpress: " + e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return temp;
    }

    public List<T> list(Terms terms) {
        return list(terms, null);
    }

    @Override
    public List<T> list(Terms terms, MultiOrder multiOrder) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<T> temp = new ArrayList<T>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            helper = dataSourceManager.getQueryPage().doQuery(thisClass, null, terms, multiOrder, null, null);
            if(ORMUtils.getDebug()) {
                logger.info("list(terms, multiOrder) : " + helper);
            }
            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            while (rs.next()) {
                T tmp = SQLHelperCreator.newClass(thisClass, rs);
                temp.add(tmp);
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "ListTerm: " +  e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS,"ListTerm: " +  e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return temp;
    }


    public boolean exists(Terms terms) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        List<String> temp = new ArrayList<String>();

        Connection conn = null;
        SQLHelper helper = null;
        try {
            helper = SQLHelperCreator.queryCount(this.thisClass, terms);
            if(ORMUtils.getDebug()) {
                logger.info("exists : " + helper);
            }
            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object tmp = rs.getObject(1);
                if(tmp != null) {
                    return Integer.parseInt(tmp.toString()) > 0;
                }
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR,"Exists: " +  e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return false;
    }

    public boolean exists(Express ... expresses) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        Connection conn = null;
        SQLHelper helper = null;
        try {
            helper = SQLHelperCreator.queryCountExpress(this.thisClass, expresses);
            if(ORMUtils.getDebug()) {
                logger.info("exists : " + helper);
            }
            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object tmp = rs.getObject(1);
                if(tmp != null) {
                    return Integer.parseInt(tmp.toString()) > 0;
                }
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR,"Exists: " +  e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return false;
    }

    public int count(Express ... expresses){

        PreparedStatement ps = null;
        ResultSet rs = null;
        Connection conn = null;
        SQLHelper helper = null;
        try {
            helper = SQLHelperCreator.queryCountExpress(this.thisClass, expresses);
            if(ORMUtils.getDebug()) {
                logger.info("Count : " + helper);
            }
            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                Object tmp = rs.getObject(1);
                if(tmp != null) {
                    return Integer.parseInt(tmp.toString());
                }
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "COUNT : " + e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return 0;
    }

//    public List<KV> list(String key, String value, Terms terms, MultiOrder multiOrder){
//        PreparedStatement ps = null;
//        ResultSet rs = null;
//        List<KV> temp = new ArrayList<KV>();
//
//        Connection conn = null;
//        SQLHelper helper = null;
//        try {
//            helper = SQLHelperCreator.query(this.thisClass, new String[]{key, value}, terms, multiOrder, null, null);
//            if(ORMUtils.getDebug()) {
//                logger.info("list(key,value,...) : " + helper);
//            }
//            conn = getConnection();
//            ps = conn.prepareStatement(helper.getSql());
//           SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
//            rs = ps.executeQuery();
//            while (rs.next()) {
//                KV kv = new KV();
//                Object k = rs.getObject(1);
//                if(k != null) {
//                    kv.setKey(k.toString());
//                }
//                Object v = rs.getObject(2);
//                if(v != null) {
//                    kv.setValue(v.toString());
//                }
//                temp.add(kv);
//            }
//        } catch (CommonException e) {
//            logger.info("SQL : " + helper);
//            throw e;
//        } catch (SQLException e) {
//            e.printStackTrace();
//            logger.info("SQL : " + helper);
//            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "LIST: " + e.getMessage());
//        } finally{
//            close(rs, ps, conn);
//        }
//        return temp;
//    }



    private <S> S getQuery(IQuery query) throws CommonException {
        S s = null;

        Connection conn = getConnection();
        //setClob通用
        QueryInfo qinfo = dataSourceManager.getQueryPage().doQuery(query, null);// = queryString(names, false);

        if(ORMUtils.getDebug()) {
            logger.info("list:" + qinfo);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(ps, qinfo.getValues(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();

            if(rs.next()){
                s = SQLHelperCreator.newClass(qinfo.getClazz(), rs);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_SECURITY, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ARGUMENT, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            close(rs, ps, conn);
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
            if(thisClass.isInstance(t)){
                helper = SQLHelperCreator.get(t);
            }else{
                helper = SQLHelperCreator.get(t, thisClass);
            }
            if(ORMUtils.getDebug()) {
                logger.info("GET : " + helper);
            }
            conn = getConnection();
            ps = conn.prepareStatement(helper.getSql());
            SQLHelperCreator.setParameter(ps, helper.getParameters(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();
            if (rs.next()) {
                temp = SQLHelperCreator.newClass(thisClass, rs);
            }
        } catch (CommonException e) {
            logger.error("SQL : " + helper, e);
            throw e;
        } catch (SQLException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "GET: " + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + helper, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "GET: " +  e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return (S)temp;
    }


    public <S> List<S> query(IQuery q) throws CommonException{
        return query(q, null);
    }

    public <S> List<S> query(IQuery q, int size) throws CommonException{

        Page page = new Page();
        page.setPage(1);
        page.setSize(size);

        Connection conn = getConnection();
        //setClob通用

        List<S> temp = new ArrayList<S>();

        QueryInfo qinfo = dataSourceManager.getQueryPage().doQuery(q, page);// = queryString(names, false);

        if(ORMUtils.getDebug()) {
            logger.info("list:" + qinfo);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(ps, qinfo.getValues(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();

            while(rs.next()){
                S t = SQLHelperCreator.newClass(qinfo.getClazz(), rs);
                temp.add(t);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_SECURITY, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ARGUMENT, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return temp;
    }

    private <S> List<S> query(IQuery q, Page page) throws CommonException{

        Connection conn = getConnection();
        //setClob通用


        List<S> temp = new ArrayList<S>();

        QueryInfo qinfo = dataSourceManager.getQueryPage().doQuery(q, page);// = queryString(names, false);

        if(ORMUtils.getDebug()) {
            logger.info("list:" + qinfo);
        }

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {

            ps = conn.prepareStatement(qinfo.getSql());
            SQLHelperCreator.setParameter(ps, qinfo.getValues(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();

            while(rs.next()){
                S t = SQLHelperCreator.newClass(qinfo.getClazz(), rs);
                temp.add(t);
            }
        } catch (SQLException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_SECURITY, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalAccessException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ACCESS, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ARGUMENT, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return temp;
    }


    public int queryCount(IQuery q) throws CommonException{
        //setClob通用

        Connection conn = null;
        int count = 0;


        PreparedStatement ps = null;
        ResultSet rs = null;
        QueryInfo qinfo = null;
        try {
            conn = getConnection();
            DatabaseType databaseType = dataSourceManager.getDatabaseType();
            qinfo = dataSourceManager.getQueryPage().doQueryCount(q);
            if(ORMUtils.getDebug()) {
                logger.info("list:" + qinfo);
            }

            String query = qinfo.getSql();// = queryString(names, false);

            ps = conn.prepareStatement(query);
            SQLHelperCreator.setParameter(ps, qinfo.getValues(), dataSourceManager.getDatabaseType(), conn);
            rs = ps.executeQuery();

            if(rs.next()){
                count = rs.getInt(1);
            }

        } catch (SQLException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (SecurityException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_SECURITY, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } catch (IllegalArgumentException e) {
            logger.error("SQL : " + qinfo, e);
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ILLEGAL_ARGUMENT, "SQL[" + qinfo.getSql() + "]" + e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return count;
    }


    public <S> Page queryPage(IQuery query, Page page) throws CommonException{
        if(page == null){
            page = new Page();
        }
        List<S> list = query(query, page);
        int total = queryCount(query);
        page.setRows(list);
        page.setTotal(total);
        return page;
    }



    /*
    public List<String> getCurrentTables(){

        List<String> temp = new ArrayList<String>();
        ResultSet rs = null;
        PreparedStatement ps = null;

        Connection conn = null;
        try {

            DatabaseType dt = getDatabaseType();

            String sql = null;
            switch(dt){
                case MYSQL:
                    //sql = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+ConnectiongetDatabaseName()+"'";
                    break;
                case ORACLE:
                    sql = "select table_name from user_tables";
                    break;
                case SQLServer:
                    break;
            }

            conn = getConnection();
            ps = conn.prepareStatement(sql);
            //DBUtil.setParameter(ps, parameters);
            rs = ps.executeQuery();

            while(rs.next()){
                String tmp = rs.getString(1);
                temp.add(tmp);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally{
            close(conn, ps, rs);
        }
        return temp;
    }*/

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
                SQLHelperCreator.setParameter(ps, objects);
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



    //注意date类型 自动转化 long
    /*public int execute(ISQLScript script, Object ... parameters){

        int res = -1;
        PreparedStatement ps = null;

        Connection conn = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(script.table());
            SQLHelperCreator.setParameter(ps, parameters);
            res = ps.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, "Execute: " + e.getMessage());
        } finally{
            close(null, ps, conn);
        }
        return res;
    }



    public String queryLatestVersion(String moduleName){
        String temp = null;
        ResultSet rs = null;
        PreparedStatement ps = null;

        Connection conn = null;
        try {

            String sql = "select version from t_sys_module where name = ? order by load_date desc";
            Object [] parameters = new Object[]{moduleName};

            //LogUtil.info("SQL:" + sql + " Parameers: " + parameters, BaseSQLImpl.class);

            conn = getConnection();
            ps = conn.prepareStatement(sql);
            SQLHelperCreator.setParameter(ps, parameters);
            rs = ps.executeQuery();
            if(rs.next()){
                temp = rs.getString(1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            //LogUtil.warn("Not any version : " + e.getMessage(), BaseSQLImpl.class);
            temp = null;
            throw new CommonException(ORMErrorCode.EXCEPTION_TYPE, ORMErrorCode.QUERY_SQL_ERROR, e.getMessage());
        } finally{
            close(rs, ps, conn);
        }
        return temp;
    }*/



}
