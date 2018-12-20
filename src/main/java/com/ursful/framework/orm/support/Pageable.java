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

import com.ursful.framework.orm.annotation.RdColumn;
import com.ursful.framework.orm.annotation.RdTable;

import java.io.Serializable;
import java.util.List;

@RdTable(name = "None", title = "分页")
public class Pageable<S> implements Serializable {

    //jqgrid page?
	@RdColumn(title = "当前页", description = "默认1", nullable = false)
	public Integer page = 1;
	@RdColumn(title = "一页数量",description = "默认10", nullable = false)
    public Integer size = 10;
    public Integer total;
    public Integer from;

	public Integer getOffset(){
		return (Math.max(1, page) - 1) * size;
	}

	public Pageable(){

	}
	public Pageable(Integer page, Integer size){
		this.page = page;
		this.size = size;
	}
	
	//private Map<String, String> orders = new HashMap<String, String>();
	//name-asc or desc

//    public Map<String, Object> attributes;
//
//	public Map<String, Object> getAttributes() {
//		return attributes;
//	}
//
//	public void setAttributes(Map<String, Object> attributes) {
//		this.attributes = attributes;
//	}

	public Integer getFrom() {
		return from;
	}

	public void setFrom(Integer from) {
		this.from = from;
	}

    public List<? extends Object> rows;

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
