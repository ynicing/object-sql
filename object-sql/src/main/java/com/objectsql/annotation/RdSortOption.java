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
/**
 #oracle
 select * from abc order by NLSSORT(NAME, 'NLS_SORT=SCHINESE_PINYIN_M') desc
 #mysql
 select * from abc order by convert(name using gbk) desc
 #sql server
 select * from abc order by NAME collate CHINESE_PRC_CI_AS desc
 #postgreSQL
 select * from abc order by convert_to(name,'GBK') desc
 #dameng
 select * from tulip_type order by NLSSORT(NAME, 'NLS_SORT=SCHINESE_PINYIN_M') desc
 */
public @interface RdSortOption {
    /**
     * 与Option内的关键字保持一致
     * @return String
     */
    String keyword();

    /**
     * 格式：RdSort默认
     * @return String
     */
    String format();
}
