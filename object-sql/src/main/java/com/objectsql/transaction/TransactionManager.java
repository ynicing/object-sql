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
package com.objectsql.transaction;

import com.objectsql.support.ChangeHolder;
import com.objectsql.utils.ORMUtils;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.DefaultTransactionStatus;

import javax.sql.DataSource;
import java.util.UUID;

public class TransactionManager extends DataSourceTransactionManager {

    public TransactionManager(DataSource dataSource){
        super(dataSource);
    }

    public TransactionManager() {
        super();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        String key = "transaction-" + UUID.randomUUID().toString();
        ORMUtils.handleDebugInfo(TransactionManager.class, "begin", key);
        ChangeHolder.set(key);
        super.doBegin(transaction, definition);
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
        ORMUtils.handleDebugInfo(TransactionManager.class, "commit", ChangeHolder.get());
        super.doCommit(status);
        ChangeHolder.change();
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
        ORMUtils.handleDebugInfo(TransactionManager.class, "rollback", ChangeHolder.get());
        ChangeHolder.remove();
        super.doRollback(status);

    }
}
