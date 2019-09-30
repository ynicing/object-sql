package com.ursful.framework.orm.support;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * <p>项目名称: ursful </p>
 * <p>描述: 列名称 </p>
 * <p>创建时间:2019/8/29 19:10 </p>
 * <p>公司信息:厦门海迈科技股份有限公司&gt;研发中心&gt;框架组</p>
 *
 * @author huangyonghua, jlis@qq.com
 */
public class Names {

    private List<String> _names = new ArrayList<String>();

    public String [] names(){
        return _names.toArray(new String[_names.size()]);
    }

    public Names(String ... names){
        if(names != null){
            _names.addAll(Arrays.asList(names));
        }
    }

    public static Names create(String ... names){
        return new Names(names);
    }

    public Names name(String name){
        _names.add(name);
        return this;
    }
}
