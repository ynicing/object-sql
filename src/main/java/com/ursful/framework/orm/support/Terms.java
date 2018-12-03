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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 类名：Condition
 * 创建者：huangyonghua
 * 日期：2017-10-19 18:11
 * 版权：ursful.com Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class Terms  implements Serializable {

    private List<ConditionObject> conditions = new ArrayList<ConditionObject>();

    public Condition getCondition(){
        Condition condition = new Condition();
        condition.setConditions(conditions);
        return condition;
    }

    public Terms and(Express ... expresses){
        if(expresses != null) {
            for(Express express : expresses) {
                conditions.add(new ConditionObject(express.getExpression(), ConditionType.AND));
            }
        }
        return this;
    }

    public Terms or(Express ... expresses){
        if(expresses != null) {
            for(Express express : expresses) {
                conditions.add(new ConditionObject(express.getExpression(), ConditionType.OR));
            }
        }
        return this;
    }

    public Terms andOr(Express ... expresses){
        if(expresses != null && expresses.length > 0) {
            int length = expresses.length;
            Expression expressions [] = new Expression[length];
            for(int i = 0; i < length; i++){
                expressions[i] = expresses[i].getExpression();
            }
            conditions.add(new ConditionObject(expressions, ConditionType.AND_OR));
        }
        return this;
    }

    public Terms orOr(Express ... expresses){
        if(expresses != null && expresses.length > 0) {
            int length = expresses.length;
            Expression expressions [] = new Expression[length];
            for(int i = 0; i < length; i++){
               expressions[i] = expresses[i].getExpression();
            }
            conditions.add(new ConditionObject(expressions, ConditionType.OR_OR));
        }
        return this;
    }

    public Terms orAnd(Express ... expresses){
        if(expresses != null && expresses.length > 0) {
            int length = expresses.length;
            Expression expressions [] = new Expression[length];
            for(int i = 0; i < length; i++){
                expressions[i] = expresses[i].getExpression();
            }
            conditions.add(new ConditionObject(expressions, ConditionType.OR_AND));
        }
        return this;
    }

}
