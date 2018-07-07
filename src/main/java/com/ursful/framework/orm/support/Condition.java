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

import com.ursful.framework.orm.utils.ORMUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 类名：Condition
 * 创建者：huangyonghua
 * 日期：2017-10-19 18:11
 * 版权：ursful.com Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class Condition {

    private List<Expression> orExpressions = new ArrayList<Expression>();

    private List<Expression> andExpressions = new ArrayList<Expression>();

    private List<List<Expression>> orAnds = new ArrayList<List<Expression>>();

    public List<List<Expression>> getOrAnds() {
        return orAnds;
    }

    public void setOrAnds(List<List<Expression>> orAnds) {
        this.orAnds = orAnds;
    }

    public List<Expression> getOrExpressions() {
        return orExpressions;
    }

    public void setOrExpressions(List<Expression> orExpressions) {
        this.orExpressions = orExpressions;
    }

    public List<Expression> getAndExpressions() {
        return andExpressions;
    }

    public void setAndExpressions(List<Expression> andExpressions) {
        this.andExpressions = andExpressions;
    }

    public Condition or(Expression expression){
        orExpressions.add(expression);
        return this;
    }

    public Condition or(Expression ... expressions){
        if(expressions != null){
            for(Expression expression : expressions){
                orExpressions.add(expression);
            }
        }
        return this;
    }

    public Condition and(Expression expression){
        andExpressions.add(expression);
        return this;
    }

    public Condition and(Expression ... expressions){
        if(expressions != null){
            for(Expression expression : expressions){
                andExpressions.add(expression);
            }
        }
        return this;
    }

    public Condition orAnd(Expression ... expressions){
        if(expressions != null){
            if(expressions.length == 1){
                or(expressions[0]);
            }else if(expressions.length > 1){
                orAnds.add(ORMUtils.newList(expressions));
            }
        }
        return this;
    }

}
