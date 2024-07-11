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
package com.objectsql.option;

import com.objectsql.IMultiQuery;
import com.objectsql.IQuery;
import com.objectsql.annotation.*;
import com.objectsql.exception.ORMException;
import com.objectsql.helper.SQLHelper;
import com.objectsql.query.QueryUtils;
import com.objectsql.support.*;
import com.objectsql.utils.ORMUtils;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

public class DMOptions extends OracleOptions{

    @Override
    public String keyword() {
        return "dm";
    }

    @Override
    public String databaseType() {
        return "DM";
    }

    @Override
    public boolean preSetParameter(PreparedStatement ps, Connection connection, String databaseType, int i,Pair pair) throws SQLException {
        Object obj = pair.getValue();
        ColumnType columnType = pair.getColumnType();
        DataType type =  DataType.getDataType(pair.getType());
        boolean hasSet = false;
        switch (type) {
            case STRING:
                if(columnType == ColumnType.BLOB){
                    if(obj != null) {
                        try {
                            ps.setBinaryStream(i + 1, new ByteArrayInputStream(obj.toString().getBytes(getCoding(pair))));
                            hasSet = true;
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }else{
                        ps.setBinaryStream(i + 1, null);
                    }
                }
                break;
            case DOUBLE:
                ps.setDouble(i + 1, (Double)obj);
                hasSet = true;
                break;
            default:
        }
        return hasSet;
    }

    public String getCaseSensitive(String name, int sensitive){
        if(name == null){
            return name;
        }
        if(Table.LOWER_CASE_SENSITIVE == sensitive){
            return name.toLowerCase(Locale.ROOT);
        }else if(Table.UPPER_CASE_SENSITIVE == sensitive){
            return name.toUpperCase(Locale.ROOT);
        }else if(Table.RESTRICT_CASE_SENSITIVE == sensitive){
            return name;
        }else{
            return name.toUpperCase(Locale.ROOT);//oracle默认大写
        }
    }
}
