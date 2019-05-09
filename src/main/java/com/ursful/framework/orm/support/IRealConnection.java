package com.ursful.framework.orm.support;

import java.sql.Connection;

/**
 * 类名：IRealConnection
 * 创建者：huangyonghua
 * 日期：2019/5/9 16:47
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public interface IRealConnection {
    Connection getConnection(Connection connection);
}
