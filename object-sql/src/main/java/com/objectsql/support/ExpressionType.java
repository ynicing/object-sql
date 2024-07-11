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
public enum ExpressionType {
    CDT_NONE, //
    CDT_EQUAL, //=
    CDT_NOT_EQUAL,//<> !=
    CDT_LIKE,// like %x% 不建议使用
    CDT_NOT_LIKE,// _a ...
    CDT_START_WITH,// x% 建议使用
    CDT_NOT_START_WITH,
    CDT_END_WITH,// %x  禁止使用 除非特定情况
    CDT_NOT_END_WITH,
    CDT_LESS,// <
    CDT_LESS_EQUAL,// <=
    CDT_MORE,// >
    CDT_MORE_EQUAL,// >=
    CDT_IN, // []/List/Collection/Set
    CDT_NOT_IN,
    CDT_IS_NULL,
    CDT_IS_NOT_NULL,
    CDT_IS_EMPTY,
    CDT_IS_NOT_EMPTY,
    CDT_EXISTS,
    CDT_NOT_EXISTS,
    CDT_BETWEEN;

    public static ExpressionType getType(String name){
        if(name != null && !"".equals(name)){
            try {
                return ExpressionType.valueOf(name.trim());
            }catch (Exception e){}
        }
        return CDT_NONE;
    }
}