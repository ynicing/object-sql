package com.ursful.framework.orm.support;

/**
 * <p>项目名称: ursful </p>
 * <p>描述:  </p>
 * <p>创建时间:2019/10/23 17:09 </p>
 * <p>公司信息:厦门海迈科技股份有限公司&gt;研发中心&gt;框架组</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public interface ICommitHandler {
    void handle(Exception e);
}
