package com.objectsql.exception;

/**
 * <p>项目名称: object-sql </p>
 * <p>描述: Table </p>
 * <p>创建时间:2020/3/13 10:02 </p>
 * <p>公司信息:objectsql</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class ORMBatchException extends ORMException{
    private Integer index;
    private Integer count;
    public ORMBatchException(Exception e){
        super(e);
    }

    public ORMBatchException(Exception e, Integer count, Integer index){
        super(e);
        this.index = index;
        this.count = count;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }
}
