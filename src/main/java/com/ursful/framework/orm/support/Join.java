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

import com.ursful.framework.orm.IQuery;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 类名：Join
 * 创建者：huangyonghua
 * 日期：2017-10-19 9:20
 * 版权：ursful.com Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class Join implements Serializable {

    private JoinType type;
    private List<Condition> conditions;
    private AliasTable table;

    public Object getTable(){
        return table.getTable();
    }

    public Join(AliasTable table){
        this(table, JoinType.LEFT_JOIN);
    }

    public Join(AliasTable table, JoinType type){
        this.table = table;
        this.type = type;
        this.conditions = new ArrayList<Condition>();
    }

    public Join on(Column column, Object value, ExpressionType expressionType){
        conditions.add(new Condition().and(new Expression(column, value, expressionType)));
        return this;
    }

    public Join on(String columnName, Object value, ExpressionType expressionType){
        conditions.add(new Condition().and(new Expression(new Column(this.table.getAlias(), columnName), value, expressionType)));
        return this;
    }

    public Join on(String columnName, Column column){
        conditions.add(new Condition().and(new Expression(new Column(this.table.getAlias(), columnName), column, ExpressionType.CDT_Equal)));
        return this;
    }

    public Join on(Column thisColumn, Column column){
        conditions.add(new Condition().and(new Expression(thisColumn, column, ExpressionType.CDT_Equal)));
        return this;
    }

    public Join on(Condition condition){
        if(condition != null) {
            conditions.add(condition);
        }
        return this;
    }

    public JoinType getType() {
        return type;
    }

    public void setType(JoinType type) {
        this.type = type;
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public void setConditions(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public String getAlias() {
        return this.table.getAlias();
    }

}
