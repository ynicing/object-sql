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
                if(express == null){
                    continue;
                }
                conditions.add(new ConditionObject(express.getExpression(), ConditionType.AND));
            }
        }
        return this;
    }

    public Terms or(Express ... expresses){
        if(expresses != null) {
            for(Express express : expresses) {
                if(express == null){
                    continue;
                }
                conditions.add(new ConditionObject(express.getExpression(), ConditionType.OR));
            }
        }
        return this;
    }

    public Terms andOr(Express ... expresses){
        expresses(ConditionType.AND_OR, expresses);
        return this;
    }

    public Terms orOr(Express ... expresses){
        expresses(ConditionType.OR_OR, expresses);
        return this;
    }

    public Terms orAnd(Express ... expresses){
        expresses(ConditionType.OR_AND, expresses);
        return this;
    }

    private void expresses(ConditionType type, Express ... expresses){
        if(expresses != null && expresses.length > 0) {
            int length = expresses.length;
            List<Expression> expressions = new ArrayList<Expression>();
            for(int i = 0; i < length; i++){
                Express express = expresses[i];
                if(express != null){
                    expressions.add(express.getExpression());
                }
            }
            conditions.add(new ConditionObject(expressions.toArray(new Expression[expressions.size()]), type));
        }
    }

}
