package com.ursful.framework.orm.transaction;

import com.ursful.framework.orm.support.ChangeHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.util.UUID;

/**
 * 类名：MultiTransactionManager
 * 创建者：huangyonghua
 * 日期：2018/8/15 8:57
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class MultiTransactionManager extends JtaTransactionManager {

    public MultiTransactionManager() {
        super();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        String key = "jta-transaction-" + UUID.randomUUID().toString();
        logger.debug("jta begin...." + key);
        ChangeHolder.set(key);
        super.doBegin(transaction, definition);
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        logger.debug("jta commit...." + ChangeHolder.get());
        super.doCommit(status);
        ChangeHolder.change();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        logger.debug("jta rollback...." + ChangeHolder.get());
        ChangeHolder.remove();
        super.doRollback(status);

    }
}
