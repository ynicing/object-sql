package com.ursful.framework.orm.support;

import java.io.Serializable;

/**
 * <p>项目名称: ursful </p>
 * <p>描述: Table Column </p>
 * <p>创建时间:2019/10/16 16:42 </p>
 * <p>公司信息:厦门海迈科技股份有限公司&gt;研发中心&gt;框架组</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class Table implements Serializable{

    private String name;
    private String comment;

    public Table(){}

    public Table(String name){
        this.name = name;
    }

    public Table(String name, String comment){
        this.name = name;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}
