package com.ursful.framework.orm.support;

import java.io.Serializable;

/**
 * 类名：ConditionObject
 * 创建者：huangyonghua
 * 日期：2018/11/30 11:13
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class ConditionObject implements Serializable{

    private Object object;
    private ConditionType type;

    public ConditionObject(){}

    public ConditionObject(Object object, ConditionType type){
        this.object = object;
        this.type = type;
    }

    public ConditionType getType() {
        return type;
    }

    public void setType(ConditionType type) {
        this.type = type;
    }

    public Object getObject() {
        return object;
    }

    public void setObject(Object object) {
        this.object = object;
    }
}
