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

package com.objectsql.annotation;

import java.lang.annotation.*;

@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RdSort {
    RdSortOption [] value() default {
            @RdSortOption(keyword = "mysql", format = "CONVERT(%s USING GBK)")
            ,@RdSortOption(keyword = "oracle", format = "NLSSORT(%s, 'NLS_SORT=SCHINESE_PINYIN_M')")
            ,@RdSortOption(keyword = "server", format = "%s COLLATE CHINESE_PRC_CI_AS")
            ,@RdSortOption(keyword = "PostgreSQL", format = "CONVERT_TO(%s,'GBK')")
            ,@RdSortOption(keyword = "dm", format = "NLSSORT(%s, 'NLS_SORT=SCHINESE_PINYIN_M')")
    };
}
