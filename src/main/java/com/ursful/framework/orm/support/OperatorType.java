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

import java.math.BigInteger;

public enum OperatorType {
    AND("&"), // &        BITAND(x, y)
    OR("|"),  // |        (x + y) - BITAND(x, y)
    XOR("^"), // ^        (x + y) - BITAND(x, y)*2   sqlserver (x + y) - (x & y)*2
    NOT("~"), // !        (x -1 ) - BITAND(x, -1)*2
    LL("<<"),   // <<     x* power(2,y)               sqlserver x* POWER(2,3)
    RR(">>"),   // >>     FLOOR(x/ power(2,y))     sqlserver FLOOR
    PLUS("+"), //+
    MINUS("-"), //-
    MULTIPLY("*"),// *
    DIVIDE("/"),// /
    MOD("%") // mod(a,2)  sqlserver %
    ;
    private String operator;

    OperatorType(String operator){
        this.operator = operator;
    }

    public String getOperator(){
        return operator;
    }
}