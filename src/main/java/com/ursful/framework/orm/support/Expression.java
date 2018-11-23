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

import java.io.Serializable;

public class Expression implements Serializable {

    public static final String EXPRESSION_ALL = "*";
    public static final String EXPRESSION_SUM = "SUM";
    public static final String EXPRESSION_MAX = "MAX";
    public static final String EXPRESSION_MIN = "MIN";
    public static final String EXPRESSION_AVG = "AVG";
    public static final String EXPRESSION_COUNT = "COUNT";

    private Column left;
	private ExpressionType type;
	private Object value;

	public Expression(Column left, Column value){
		this.left = left;
		this.type = ExpressionType.CDT_Equal;
		this.value = value;
	}

    public Expression(Column left, ExpressionType type){
        this.left = left;
        this.type = type;
    }

	public Expression(Column left, Object value, ExpressionType type){
		this.left = left;
		this.type = type;
		this.value = value;
	}

	public Column getLeft() {
		return left;
	}

    /**
     * set
     * @param left
     */
	public void setLeft(Column left) {
		this.left = left;
	}

	public ExpressionType getType() {
		return type;
	}
	public void setType(ExpressionType type) {
		this.type = type;
	}
	public Object getValue() {
		return value;
	}
	public void setValue(Object value) {
		this.value = value;
	}

    public static void main(String[] args) {

    }
}