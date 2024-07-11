package com.objectsql.exception;

import java.util.HashMap;
import java.util.Map;

/**
 * <p>项目名称: object-sql </p>
 * <p>描述: Table </p>
 * <p>创建时间:2020/3/13 10:02 </p>
 * <p>公司信息:objectsql</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class ORMSQLException extends ORMException{

    private Map<String, Object> params;

    public Map<String, Object> getParams() {
        return params;
    }

    public ORMSQLException(Exception exception, String msg, Object... args){
        super(exception.getMessage() != null? (msg + ":" + exception.getMessage()):msg);
    }

    public ORMSQLException(Exception exception, String msg){
        super(exception.getMessage() != null? (msg + ":" + exception.getMessage()):msg);
    }

    public ORMSQLException(String msg){
        super(msg);
    }

    public ORMSQLException put(String key, Object value){
        if(this.params == null){
            this.params = new HashMap<String, Object>();
        }
        this.params.put(key, value);
        return this;
    }
}
