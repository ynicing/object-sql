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
package com.ursful.framework.core.exception;

import com.ursful.framework.core.error.ErrorCode;

public class CommonException extends RuntimeException {

	private static final long serialVersionUID = 3702807659429166051L;

	private ErrorCode errorCode;

    private String type;

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(ErrorCode errorCode) {
        this.errorCode = errorCode;
    }

    private String message;

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public CommonException(ErrorCode errorCode, String message){
		super(message);
		this.errorCode = errorCode;
		this.message = message;
	}

    public CommonException(String type, ErrorCode errorCode, String message){
        super(message);
        this.type = type;
        this.errorCode = errorCode;
        this.message = message;
    }
}
