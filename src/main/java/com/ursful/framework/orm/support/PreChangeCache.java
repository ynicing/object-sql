package com.ursful.framework.orm.support;

import com.ursful.framework.orm.listener.IChangedListener;

/**
 * 类名：PreChangeCache
 * 创建者：huangyonghua
 * 日期：2018/8/15 9:17
 * 版权：Hymake Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */
public class PreChangeCache {

    private IChangedListener changedListener;
    private Object original;
    private Object current;

    public void changed(){
        changedListener.changed(original, current);
    }

    public PreChangeCache(IChangedListener listener, Object original, Object current){
        this.changedListener = listener;
        this.original = original;
        this.current = current;
    }

    public IChangedListener getChangedListener() {
        return changedListener;
    }

    public void setChangedListener(IChangedListener changedListener) {
        this.changedListener = changedListener;
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
