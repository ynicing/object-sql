package com.ursful.framework.orm.transaction;

import com.ursful.framework.orm.support.ChangeHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;

import javax.sql.DataSource;
import java.util.UUID;

/**
 * 类名：TransactionManager
 * 创建者：huangyonghua
 * 日期：2018/8/15 8:38
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class TransactionManager extends DataSourceTransactionManager {

    private static Logger logger = LoggerFactory.getLogger(TransactionManager.class);

    public TransactionManager(DataSource dataSource){
        super(dataSource);
    }

    public TransactionManager() {
        super();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        String key = "transaction-" + UUID.randomUUID().toString();
        logger.debug("begin...." + key);
        ChangeHolder.set(key);
        super.doBegin(transaction, definition);
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        logger.debug("commit...." + ChangeHolder.get());
        super.doCommit(status);
        ChangeHolder.change();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        logger.debug("rollback...." + ChangeHolder.get());
        ChangeHolder.remove();
        super.doRollback(status);

    }
}
