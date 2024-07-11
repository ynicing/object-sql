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

import java.util.Locale;

public abstract class DatabaseTypeHolder {

	private static final ThreadLocal<String> threadLocal = new ThreadLocal<String>();

	public static void remove() {
        threadLocal.remove();
	}

	public static void set(String type) {
		if (type == null) {
			remove();
		}else {
            threadLocal.set(type.toUpperCase(Locale.ROOT));
		}
	}

	public static String get() {
        return threadLocal.get();
	}



}