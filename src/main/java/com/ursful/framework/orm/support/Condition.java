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

public class Condition implements Serializable {

    private List<ConditionObject> conditions = new ArrayList<ConditionObject>();

    public List<ConditionObject> getConditions() {
        return conditions;
    }

    public void setConditions(List<ConditionObject> conditions) {
        this.conditions = conditions;
    }

    public Condition and(Expression ... expressions){
        if(expressions != null) {
            for(Expression expression: expressions) {
                if(expression == null){
                    continue;
                }
                conditions.add(new ConditionObject(expression, ConditionType.AND));
            }
        }
        return this;
    }

    public Condition or(Expression  ... expressions){
        if(expressions != null) {
            for(Expression expression: expressions) {
                if(expression == null){
                    continue;
                }
                conditions.add(new ConditionObject(expression, ConditionType.OR));
            }
        }
        return this;
    }

    public Condition or(Condition condition){
        if(condition != null) {
            conditions.add(new ConditionObject(condition, ConditionType.OR));
        }
        return this;
    }

    public Condition and(Condition condition){
        if(condition != null) {
            conditions.add(new ConditionObject(condition, ConditionType.AND));
        }
        return this;
    }

    public Condition orOr(Expression ... expressions){
        if(expressions != null){
            conditions.add(new ConditionObject(expressions, ConditionType.OR_OR));
        }
        return this;
    }

    public Condition orAnd(Expression ... expressions){
        if(expressions != null){
            conditions.add(new ConditionObject(expressions, ConditionType.OR_AND));
        }
        return this;
    }

    public Condition andOr(Expression ... expressions){
        if(expressions != null){
            conditions.add(new ConditionObject(expressions, ConditionType.AND_OR));
        }
        return this;
    }


    public Condition and(SQLPair pair){
        if(pair != null) {
            conditions.add(new ConditionObject(pair, ConditionType.AND));
        }
        return this;
    }

    public Condition or(SQLPair pair){
        if(pair != null) {
            conditions.add(new ConditionObject(pair, ConditionType.OR));
        }
        return this;
    }
}
