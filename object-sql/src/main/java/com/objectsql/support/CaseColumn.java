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

package com.objectsql.support;

import java.util.LinkedHashMap;
import java.util.Map;

public class CaseColumn extends Column {

    public CaseColumn() {
        this.conditions = conditions;
        this.elseValue = elseValue;
    }

    public CaseColumn(String function, String alias, String name, String asName) {
        super(function, alias, name, asName);
        this.conditions = conditions;
        this.elseValue = elseValue;
    }

    private Map<Condition, Object> conditions = new LinkedHashMap<Condition, Object>();
    private Object elseValue;

    public CaseColumn whenThen(Expression expression, Object value){
        conditions.put(Condition.create().and(expression), value);
        return this;
    }

    public CaseColumn whenThen(Condition condition, Object value){
        conditions.put(condition, value);
        return this;
    }

    public CaseColumn elseEnd(Object value){
        this.elseValue = value;
        return this;
    }

    public Object getElseValue() {
        return elseValue;
    }

    public void setElseValue(Object elseValue) {
        this.elseValue = elseValue;
    }

    public Map<Condition, Object> getConditions() {
        return conditions;
    }

    public void setConditions(Map<Condition, Object> conditions) {
        this.conditions = conditions;
    }
}