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

import com.objectsql.annotation.RdColumn;
import com.objectsql.annotation.RdTable;

import java.io.Serializable;
import java.util.List;

@RdTable(name = "None", comment = "分页")
public class Pageable<S> implements Serializable {

	@RdColumn(title = "当前页", description = "默认1", nullable = false)
	public Integer page = 1;
	@RdColumn(title = "分页大小",description = "默认10", nullable = false)
    public Integer size = 10;
	@RdColumn(title = "总数", nullable = false)
    public Integer total;
	@RdColumn(title = "偏移")
	public Integer offset;

	@RdColumn(title = "数据列表")
	public List<? extends Object> rows;

	public void setOffset(Integer offset) {
		this.offset = offset;
	}

	//优先，使用offset
	public Integer getOffset(){
		if(offset != null){
			return offset;
		}
		if(page == null){
			page = 1;
		}
		if(size == null){
			size = 10;
		}
		return (Math.max(1, page) - 1) * size;
	}

	public Pageable(){

	}
	public Pageable(Integer page, Integer size){
		this.page = page;
		this.size = size;
	}

	public Integer getPage() {
		return page;
	}

	public void setPage(Integer page) {
		this.page = page;
	}

	public Integer getSize() {
		return size;
	}

	public void setSize(Integer size) {
		this.size = size;
	}

	public Integer getTotal() {
		return total;
	}

	public void setTotal(Integer total) {
		this.total = total;
	}

	public <T> List<T> getRows() {
		return (List<T>)this.rows;
	}

	public void setRows(List<? extends Object> rows) {
		this.rows = rows;
	}
	
}
