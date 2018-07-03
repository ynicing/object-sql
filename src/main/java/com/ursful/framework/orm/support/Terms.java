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

import java.util.ArrayList;
import java.util.List;

/**
 * 类名：Condition
 * 创建者：huangyonghua
 * 日期：2017-10-19 18:11
 * 版权：厦门维途信息技术有限公司 Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class Terms {

    private List<Expression> orExpressions = new ArrayList<Expression>();
    private List<Expression> andExpressions = new ArrayList<Expression>();

    public Condition getCondition(){
        Condition condition = new Condition();
        condition.setAndExpressions(andExpressions);
        condition.setOrExpressions(orExpressions);
        return condition;
    }

    public Terms or(Express express){
        orExpressions.add(express.getExpression());
        return this;
    }

    public Terms and(Express express){
        andExpressions.add(express.getExpression());
        return this;
    }
}
