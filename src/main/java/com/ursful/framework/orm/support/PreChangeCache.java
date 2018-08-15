package com.ursful.framework.orm.support;

import com.ursful.framework.orm.IBaseService;

/**
 * 类名：PreChangeCache
 * 创建者：huangyonghua
 * 日期：2018/8/15 9:17
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class PreChangeCache {

    private IBaseService baseService;
    private Object original;
    private Object current;

    public void changed(){
        baseService.changed(original, current);
    }

    public PreChangeCache(IBaseService service, Object original, Object current){
        this.baseService = service;
        this.original = original;
        this.current = current;
    }

    public IBaseService getBaseService() {
        return baseService;
    }

    public void setBaseService(IBaseService baseService) {
        this.baseService = baseService;
    }

    public Object getOriginal() {
        return original;
    }

    public void setOriginal(Object original) {
        this.original = original;
    }

    public Object getCurrent() {
        return current;
    }

    public void setCurrent(Object current) {
        this.current = current;
    }
}
