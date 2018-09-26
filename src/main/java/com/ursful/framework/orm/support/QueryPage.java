package com.ursful.framework.orm.support;

import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.helper.SQLHelper;

/**
 * Created by Administrator on 2018/7/5.
 */
public interface QueryPage {
    DatabaseType databaseType();
    QueryInfo doQueryCount(IQuery query);
    QueryInfo doQuery(IQuery query, Pageable page);
    SQLHelper doQuery(Class<?> clazz, String [] names, Terms terms, MultiOrder multiOrder, Integer start, Integer size);
}
