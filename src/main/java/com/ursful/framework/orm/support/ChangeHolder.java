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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class ChangeHolder {

	private static final ThreadLocal<String> threadLocal = new ThreadLocal<String>();

	private static Map<String, List<PreChangeCache>> changeCacheMap = new HashMap<String, List<PreChangeCache>>();

	public static void cache(PreChangeCache cache){
		String key = get();
		List<PreChangeCache> caches = changeCacheMap.get(key);
		if(caches == null){
			caches = new ArrayList<PreChangeCache>();
		}
		caches.add(cache);
		changeCacheMap.put(key, caches);
	}

	public static void change(){
		String key = get();
		List<PreChangeCache> changeCaches = changeCacheMap.get(key);
		if(changeCaches != null){
			for(PreChangeCache changeCache : changeCaches) {
				changeCache.changed();
			}
		}
		remove();
	}

	public static void remove() {
		String key = threadLocal.get();
		changeCacheMap.remove(key);
        threadLocal.remove();
	}

	public static void set(String change) {
		if (change == null) {
			remove();
		}else {
            threadLocal.set(change);
		}
	}

	public static String get() {
        return threadLocal.get();
	}



}