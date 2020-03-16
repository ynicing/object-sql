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
package com.ursful.framework.orm.transaction;

import com.ursful.framework.orm.support.ChangeHolder;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

import java.util.UUID;

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
