package com.ursful.framework.orm.page;

import com.ursful.framework.orm.support.*;

public class H2QueryPage extends MySQLQueryPage{
    @Override
    public DatabaseType databaseType() {
        return DatabaseType.H2;
    }

}
