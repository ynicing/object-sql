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

public class Express {

    private Column left;
	private ExpressionType type;
	private Object value;


    public Express(String left, ExpressionType type){
        this.left = new Column(left);
        this.type = type;
    }

    //solo.
    public Express(String left, Object value, ExpressionType type){
        this.left = new Column(left);
        this.type = type;
        this.value = value;
    }

    public Expression getExpression(){
        return new Expression(this.left, value, type);
    }

    public Express(String function, String left, Object value, ExpressionType type){
        this.left = new Column(function, null, left, null);
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