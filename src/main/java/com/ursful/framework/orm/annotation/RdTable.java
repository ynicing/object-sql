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
import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RdTable {

	public static final int DEFAULT_SENSITIVE = 0;
	public static final int UPPER_CASE_SENSITIVE = 1;
	public static final int LOWER_CASE_SENSITIVE = 2;
	public static final int RESTRICT_CASE_SENSITIVE = 3;

	String name();//user/create  user
	String schema() default "";
	@Deprecated
	String title() default "";
	String comment() default "";
	boolean dropped() default false;
	//MySQL
	String engine() default "InnoDB";
	String collate() default "utf8mb4_bin";

 	int sensitive() default DEFAULT_SENSITIVE;
}
