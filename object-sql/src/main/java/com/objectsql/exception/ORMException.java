package com.objectsql.exception;

/**
 * <p>项目名称: object-sql </p>
 * <p>描述: Table </p>
 * <p>创建时间:2020/3/13 10:02 </p>
 * <p>公司信息:objectsql</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class ORMException extends RuntimeException{

    public ORMException(String msg){
        super(msg);
    }

    public ORMException(Exception e){
        super(e);
    }


    public ORMException(String msg, Exception e){
        super(msg, e);
    }
}
