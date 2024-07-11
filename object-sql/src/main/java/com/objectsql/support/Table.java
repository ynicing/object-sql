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

import com.objectsql.annotation.RdTable;

import java.io.Serializable;

public class Table implements Serializable{

    public static final int DEFAULT_SENSITIVE = 0;
    public static final int UPPER_CASE_SENSITIVE = 1;
    public static final int LOWER_CASE_SENSITIVE = 2;
    public static final int RESTRICT_CASE_SENSITIVE = 3;

    private String name;
    private String comment;
    private int sensitive = DEFAULT_SENSITIVE;

    //mysql
    private String engine = "InnoDB";
    private String collate = "utf8mb4_bin";

    public String getEngine() {
        return engine;
    }

    public void setEngine(String engine) {
        this.engine = engine;
    }

    public String getCollate() {
        return collate;
    }

    public void setCollate(String collate) {
        this.collate = collate;
    }

    public Table(){}

    public Table(String name){
        this.name = name;
    }

    public Table(String name, String comment){
        this.name = name;
        this.comment = comment;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getSensitive() {
        return sensitive;
    }

    public void setSensitive(int sensitive) {
        this.sensitive = sensitive;
    }

    @Override
    public String toString() {
        return "Table{" +
                "name='" + name + '\'' +
                ", comment='" + comment + '\'' +
                '}';
    }
}
