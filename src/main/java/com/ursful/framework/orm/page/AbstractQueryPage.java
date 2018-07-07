package com.ursful.framework.orm.page;

import com.ursful.framework.core.exception.CommonException;
import com.ursful.framework.orm.IQuery;
import com.ursful.framework.orm.annotation.RdTable;
import com.ursful.framework.orm.query.QueryUtils;
import com.ursful.framework.orm.support.*;
import com.ursful.framework.orm.utils.ORMUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractQueryPage implements QueryPage{

    public String getWordAfterFrom(IQuery query, List<Pair> values, boolean count, String baseName) throws CommonException{
        StringBuffer sb = new StringBuffer();
        sb.append(" FROM ");

        if(query.getTable() != null){
            RdTable table = (RdTable)query.getTable().getAnnotation(RdTable.class);
            sb.append(table.name());
            if(baseName != null){
                sb.append(" " +baseName);
            }
        }else{
            List<String> words = new ArrayList<String>();
            Map<String, Class<?>> aliasMap = query.getAliasTable();
            Map<String, IQuery> aliasQuery = query.getAliasQuery();
            List<String> aliasList = query.getAliasList();
            for(String alias : aliasList) {
                if(aliasMap.containsKey(alias)) {
                    RdTable table = (RdTable) aliasMap.get(alias).getAnnotation(RdTable.class);
                    words.add(table.name() + " " + alias);
                }else if(aliasQuery.containsKey(alias)){
                    IQuery q = aliasQuery.get(alias);
                    QueryInfo queryInfo = doQuery(q, null);
                    words.add("(" + queryInfo.getSql() + ") " + alias);
                    values.addAll(queryInfo.getValues());
                }
            }
            sb.append(ORMUtils.join(words, ","));
        }

        String join = join(query.getJoins(), values);
        if(join != null && !"".equals(join)){
            sb.append(join);
        }

        String whereCondition = QueryUtils.getConditions(query.getConditions(), values);
        if(whereCondition != null && !"".equals(whereCondition)){
            sb.append(" WHERE " + whereCondition);
        }

        String groupString = QueryUtils.getGroups(query.getGroups());

        if(groupString != null && !"".equals(groupString)){
            sb.append(" GROUP BY ");
            sb.append(groupString);
        }

        String havingString = QueryUtils.getConditions(query.getHavings(), values);
        if(havingString != null && !"".equals(havingString)){
            sb.append(" HAVING ");
            sb.append(havingString);
        }

        if(!count) {
            String orderString = QueryUtils.getOrders(query.getOrders());
            if (orderString != null && !"".equals(orderString)) {
                sb.append(" ORDER BY ");
                sb.append(orderString);
            }
        }

        return sb.toString();

    }

    public String join(List<Join> joins, List<Pair> values) throws CommonException {
        StringBuffer sb = new StringBuffer();
        if(joins == null){
            return  sb.toString();
        }
        for(int i = 0; i < joins.size(); i++){
            Join join = joins.get(i);
            String tableName = null;
            Object table = join.getTable();
            if(table instanceof Class) {
                RdTable rdTable = (RdTable)((Class<?>)table).getAnnotation(RdTable.class);
                if(rdTable == null){
                    continue;
                }
                tableName = rdTable.name();
            }else if(table instanceof IQuery){
                QueryInfo info = doQuery((IQuery)table, null);
                tableName = "(" + info.getSql() + ") ";
                values.addAll(info.getValues());
            }

            switch (join.getType()){
                case FULL_JOIN:
                    sb.append(" FULL JOIN ");
                    break;
                case INNER_JOIN:
                    sb.append(" INNER JOIN ");
                    break;
                case LEFT_JOIN:
                    sb.append(" LEFT JOIN ");
                    break;
                case RIGHT_JOIN:
                    sb.append(" RIGHT JOIN ");
                    break;
            }
            String alias = join.getAlias();

            sb.append(tableName + " " + alias);

            List<Condition> temp = join.getConditions();

            String cdt = QueryUtils.getConditions(temp, values);
            if(cdt != null && !"".equals(cdt)) {
                sb.append(" ON ");
                sb.append(cdt);
            }
        }
        return sb.toString();
    }
}
