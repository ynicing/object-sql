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

package com.ursful.framework.orm.error;

import com.ursful.framework.core.error.ErrorCode;

public enum ORMErrorCode implements ErrorCode{

    TABLE_NOT_FOUND(1001, "table.not.found.in.class"),
    TABLE_COLUMN_NOT_ALLOW_NULL(1002, "table.column.not.allow.null"),
    TABLE_DELETE_WITHOUT_ID(1003, "table.delete.without.id"),
    TABLE_UPDATE_WITHOUT_ID(1004, "table.update.without.id"),
    TABLE_GET_WITHOUT_ID(1005, "table.get.without.id"),
    TABLE_SAVE_WITHOUT_VALUE(1006, "table.save.without.value"),
    TABLE_WHEN_DELETE_SHOULD_DO_WITH_PARAMETER(1007, "table.when.delete.should.do.with.parameter"),
    TABLE_SET_PARAMETER_ILLEGAL_ARGUMENT(1008, "table.set.parameter.illegal.argument"),
    TABLE_SET_PARAMETER_ILLEGAL_ACCESS(1009, "table.set.parameter.illegal.access"),
    TABLE_QUERY_NAMES_AS_NOT_EQUAL(1010, "table.create.query.names.as.not.equal"),
    TABLE_DELETE_WITHOUT_EXPRESS(1011, "table.delete.without.express"),
    QUERY_SQL_ERROR(1020, "query.sql.error"),
    QUERY_SQL_COLUMN_IS_NULL(1020, "query.sql.column.is.null"),
    QUERY_SQL_CLASS_HAS_NOT_TABLE(1020, "query.sql.class.has.not.table"),
    QUERY_SQL_CLASS_HAS_NOT_ANY_COLUMN(1020, "query.sql.class.has.not.any.column"),
    QUERY_SQL_NAME_IS_NULL(1021, "query.sql.name.is.null"),
    QUERY_SQL_SELECT_ERROR(1022, "query.names.as.not.equal"),
    QUERY_SQL_SECURITY(1023, "query.sql.security"),
    QUERY_SQL_ILLEGAL_ARGUMENT(1024, "query.sql.illegal.access"),
    QUERY_SQL_ILLEGAL_ACCESS(1025, "query.sql.illegal.access"),
    BATCH_EXECUTE_ERROR(1030, "batch.execute.error"),

    ;

    public static final String EXCEPTION_TYPE = "ORM";

    private Integer code;
    private String message;

    ORMErrorCode(Integer code, String message){
        this.code = code;
        this.message = message;
    }

    @Override
    public Integer code() {
        return this.code;
    }

    @Override
    public String message() {
        return this.message;
    }
}
