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

import com.objectsql.support.ColumnType;

import java.lang.annotation.*;


@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RdColumn {
	//base info
	String name() default "";
	boolean nullable() default true;
	ColumnType type() default ColumnType.NULL;
	//comment = title:description
	String title() default "";
	String description() default "";
	String defaultValue() default "";
	int order() default 0;

	//drop
	boolean dropped() default false;

	//String
	int length() default 191;
	String coding() default "UTF-8";

	//number
	int precision() default 0;//date(Long)=15
	int scale() default 0;
	int runningMode() default -1;//BigDecimal
}
