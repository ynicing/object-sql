/*
 * Copyright 2017 @objectsql.com
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
package com.objectsql.support;

import com.objectsql.listener.IServiceChangedListener;

public class PreChangeCache {

    private IServiceChangedListener changedListener;

    private ORMType ormType;
    private ORMOption ormOption;

    public void changed(){
        changedListener.changed(ormType, ormOption);
    }

    public PreChangeCache(){}

    public PreChangeCache(IServiceChangedListener listener, ORMType ormType, ORMOption option){
        this.changedListener = listener;
        this.ormType = ormType;
        this.ormOption = option;
    }

    public IServiceChangedListener getChangedListener() {
        return changedListener;
    }

    public void setChangedListener(IServiceChangedListener changedListener) {
        this.changedListener = changedListener;
    }

    public Object getOriginal() {
        if (this.getOrmOption() != null){
            return this.getOrmOption().getOriginal();
        }
        return null;
    }
    public Object getCurrent() {
        if (this.getOrmOption() != null){
            return this.getOrmOption().getCurrent();
        }
        return null;
    }


    public ORMType getOrmType() {
        return ormType;
    }

    public void setOrmType(ORMType ormType) {
        this.ormType = ormType;
    }

    public ORMOption getOrmOption() {
        return ormOption;
    }

    public void setOrmOption(ORMOption ormOption) {
        this.ormOption = ormOption;
    }
}
