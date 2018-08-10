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

/**
 * 枚举：LargeString
 * 创建者：huangyonghua
 * 日期：2017-10-20 18:24
 * 版权：ursful.com Copyright(c) 2017
 * 说明：[类说明必填内容，请修改]
 */

public enum ColumnType {
    NULL,//空，默认选项
    BLOB,//数据库Blob
    CLOB,//数据库Clob
    BINARY,//数据库二进制流
    LONG,//与Date使用，时间转long
    DATETIME,//数据库DateTime 或者 Date（Oracle）
    TIMESTAMP;//数据库Timestamp
}
