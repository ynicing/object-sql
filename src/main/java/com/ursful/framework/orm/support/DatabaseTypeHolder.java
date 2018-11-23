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

public abstract class DatabaseTypeHolder {

	private static final ThreadLocal<DatabaseType> threadLocal = new ThreadLocal<DatabaseType>();

	public static void remove() {
        threadLocal.remove();
	}

	public static void set(DatabaseType type) {
		if (type == null) {
			remove();
		}else {
            threadLocal.set(type);
		}
	}

	public static DatabaseType get() {
        return threadLocal.get();
	}



}