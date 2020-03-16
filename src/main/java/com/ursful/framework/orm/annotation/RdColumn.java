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
package com.ursful.framework.orm.annotation;

import com.ursful.framework.orm.support.ColumnType;

import java.lang.annotation.*;


@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RdColumn {
	String name() default "";
	boolean nullable() default true;
	boolean dropped() default false;
	int length() default 255;//不限制
    ColumnType type() default ColumnType.NULL;
	String title() default "";
	String description() default "";
	int order() default 0;
	int datePrecision() default 15;
	int precision() default 0;
	int scale() default 0;
	String defaultValue() default "";
	String coding() default "UTF-8";
	int runningMode() default -1;//BigDecimal

	@Deprecated
	boolean unique() default false;
	@Deprecated
	String uniqueName() default "";
	@Deprecated
	String [] uniqueKeys() default {};
	@Deprecated
	String foreignKey() default "";
	@Deprecated
	String foreignTable() default "";
	@Deprecated
	String foreignColumn() default "";
}
