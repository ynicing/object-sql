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
package com.ursful.framework.orm.support;

import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

/**
 * 类名：AutoCommit
 * 创建者：huangyonghua
 * 日期：2017-11-23 15:40
 * 版权：ursful.com Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class Commit {

    private PlatformTransactionManager transactionManager;
    private TransactionStatus status;
    private boolean executeSuccess = false;
    private ICommitHandler commitHandler;

    public Commit(Object manager){
        this.transactionManager = (PlatformTransactionManager)manager;
    }

    public void execute(Runnable runnable){
        execute(runnable, null);
    }
    public void execute(Runnable runnable, ICommitHandler commitHandler){
        if(runnable == null){
            return;
        }
        try{
            manual();
            runnable.run();
            commit();
        }catch (Exception e){
            if(commitHandler != null){
                commitHandler.handle(e);
            }
        }finally {
            if(!executeSuccess) {
                rollback();
            }
        }
    }

    private void manual(){
        DefaultTransactionDefinition definition = new DefaultTransactionDefinition();
        definition.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        status = this.transactionManager.getTransaction(definition);
        executeSuccess = false;
    }

    private void commit(){
        if(transactionManager != null && status != null ){
            transactionManager.commit(status);
            executeSuccess = true;
        }
    }

    private void rollback(){
        if(!executeSuccess && transactionManager != null && status != null ){
            transactionManager.rollback(status);
        }
    }
}
