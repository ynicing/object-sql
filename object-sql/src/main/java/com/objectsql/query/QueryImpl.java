package com.objectsql.query;

import com.objectsql.IQuery;
import com.objectsql.handler.IQueryConvert;
import com.objectsql.support.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
public abstract class QueryImpl implements IQuery{

    private String id;
    private Pageable pageable;
    protected boolean distinct = false;
    protected List<Condition> conditions = new ArrayList<Condition>();
    protected List<Condition> havings = new ArrayList<Condition>();
    protected boolean dataPermission = true;
    protected List<Order> orders = new ArrayList<Order>();
    protected TextTransformType textTransformType = TextTransformType.UPPER;
    protected boolean lessDatePlus = false;
    protected boolean lessEqualDatePlus = false;
    protected Options options;

    private IQueryConvert queryConvert;

    @Override
    public IQueryConvert getQueryConvert() {
        return queryConvert;
    }

    @Override
    public void setQueryConvert(IQueryConvert queryConvert) {
        this.queryConvert = queryConvert;
    }

    protected void addCondition(Condition condition){
        if (condition != null){
            resetLessOrLessEqualDate(condition);
            conditions.add(condition);
        }
    }

    protected void addHaving(Condition condition) {
        if (condition != null) {
            resetLessOrLessEqualDate(condition);
            havings.add(condition);
        }
    }

    private void resetLessOrLessEqualDate(Condition condition){
        List<ConditionObject> conditionObjects = condition.getConditions();
        for (ConditionObject conditionObject : conditionObjects){
            Object object = conditionObject.getObject();
            if(object instanceof Condition){
                resetLessOrLessEqualDate((Condition)object);
            }else if(object instanceof Expression){
                resetLessOrLessEqualDate((Expression)object);
            }else if(object instanceof Expression[]){
                Expression [] expressions = (Expression[]) object;
                for (Expression expression: expressions){
                    resetLessOrLessEqualDate(expression);
                }
            }
        }
    }

    private void resetLessOrLessEqualDate(Expression expression){
        if(expression != null){
            if(expression.getType() == ExpressionType.CDT_LESS){
                if(isLessDatePlus235959() && (expression.getValue() instanceof Date)){
                    expression.setValue(QueryUtils.plusDate235959((Date)expression.getValue()));
                }
            }else if(expression.getType() == ExpressionType.CDT_LESS_EQUAL){
                if(isLessEqualDatePlus235959() && (expression.getValue() instanceof Date)){
                    expression.setValue(QueryUtils.plusDate235959((Date)expression.getValue()));
                }
            }
        }
    }

    @Override
    public String id() {
        return this.id;
    }

    @Override
    public void setId(String id){
        this.id = id;
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public List<Condition> getConditions() {
        return conditions;
    }

    @Override
    public List<Condition> getHavings() {
        return havings;
    }

    @Override
    public List<Order> getOrders() {
        return orders;
    }

    @Override
    public void setDataPermission(boolean allowed) {
        this.dataPermission = allowed;
    }

    @Override
    public boolean dataPermission() {
        return this.dataPermission;
    }

    @Override
    public TextTransformType textTransformType() {
        return textTransformType;
    }

    @Override
    public void setTextTransformType(TextTransformType textTransformType) {
        this.textTransformType = textTransformType;
    }

    public boolean isLessEqualDatePlus235959(){
        return lessEqualDatePlus;
    }

    public boolean isLessDatePlus235959(){
        return lessDatePlus;
    }

    public void enableLessOrLessEqualDatePlus235959(){
        this.lessDatePlus = true;
        this.lessEqualDatePlus = true;
    }

    public void enableLessDatePlus235959(){
        this.lessDatePlus = true;
    }

    public void enableLessEqualDatePlus235959(){
        this.lessEqualDatePlus = true;
    }

    @Override
    public Pageable getPageable() {
        return this.pageable;
    }

    @Override
    public void setPageable(Pageable pageable) {
        this.pageable = pageable;
    }

    @Override
    public void setOptions(Options options) {
        this.options = options;
    }

    @Override
    public Options getOptions() {
        return this.options;
    }
}
