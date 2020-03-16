/*
 * Copyright 2017 @ursful.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ursful.framework.orm.support;

import com.ursful.framework.orm.listener.IServiceChangedListener;

public class PreChangeCache {

    private IServiceChangedListener changedListener;
    private Object original;
    private Object current;

    public void changed(){
        changedListener.changed(original, current);
    }

    public PreChangeCache(){}

    public PreChangeCache(IServiceChangedListener listener, Object original, Object current){
        this.changedListener = listener;
        this.original = original;
        this.current = current;
    }

    public IServiceChangedListener getChangedListener() {
        return changedListener;
    }

    public void setChangedListener(IServiceChangedListener changedListener) {
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
