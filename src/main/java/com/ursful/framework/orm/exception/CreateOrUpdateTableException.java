package com.ursful.framework.orm.exception;

/**
 * <p>项目名称: ursful-orm </p>
 * <p>描述: Table </p>
 * <p>创建时间:2020/3/13 10:02 </p>
 * <p>公司信息:厦门海迈科技股份有限公司&gt;研发中心&gt;框架组</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class CreateOrUpdateTableException extends Exception{
    public CreateOrUpdateTableException(){
        super("Create or update table Exception.");
    }

    public CreateOrUpdateTableException(String msg){
        super("Create or update table Exception : " + msg);
    }
}
