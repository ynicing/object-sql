package com.objectsql.support;

import java.io.Serializable;

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
