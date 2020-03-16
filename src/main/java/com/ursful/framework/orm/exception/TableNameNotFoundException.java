package com.ursful.framework.orm.exception;

/**
 * <p>项目名称: ursful-orm </p>
 * <p>描述: Table name not found </p>
 * <p>创建时间:2020/3/11 17:06 </p>
 * <p>公司信息:厦门海迈科技股份有限公司&gt;研发中心&gt;框架组</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class TableNameNotFoundException extends CreateOrUpdateTableException {
    public TableNameNotFoundException(){
        super("Table name not found.");
    }


}
