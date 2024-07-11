package com.objectsql.generator;

import com.objectsql.BaseServiceImpl;
import com.objectsql.IBaseService;
import com.objectsql.ISQLService;
import com.objectsql.ObjectSQLManager;
import com.objectsql.annotation.RdColumn;
import com.objectsql.annotation.RdId;
import com.objectsql.annotation.RdTable;
import com.objectsql.exception.ORMException;
import com.objectsql.query.QueryUtils;
import com.objectsql.support.ColumnType;
import com.objectsql.support.Table;
import com.objectsql.support.TableColumn;
import com.objectsql.utils.ORMUtils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.util.*;

public class CodeGenerator {

    private static final String FOUR_SPACE = "    ";

    public static String generateEntity(String tableName,
                                  ISQLService sqlService,
                                  boolean ignorePrefix,
                                  Class baseEntity,
                                  Map<String, String> replaceClasses) {
        Table table = sqlService.table(tableName);
        if (table == null){
            throw new ORMException("Table not found : " + tableName);
        }

        List<TableColumn> columns = sqlService.tableColumns(tableName);
        if (columns == null || columns.isEmpty()){
            throw new ORMException("Table columns not found : " + tableName);
        }
        List<String> contents = new ArrayList<String>();

        //add import
        contents.add(String.format("import %s;", RdColumn.class.getName()));
        contents.add(String.format("import %s;", RdId.class.getName()));
        contents.add(String.format("import %s;", RdTable.class.getName()));
        contents.add(String.format("import %s;", ColumnType.class.getName()));

        int insertIndex = contents.size();
        //判断是否继承 BaseEntity
        List<String> baseEntityInfo = getBaseEntityInfo(baseEntity);

        boolean useExtendEntity = contains(columns, replaceClasses, baseEntityInfo);
        if(useExtendEntity && !baseEntityInfo.isEmpty()){
            if(baseEntity != null){
                contents.add(String.format("import %s;", baseEntity.getName()));
            }
        }else{
            contents.add(String.format("import %s;", Serializable.class.getName()));
        }

        List<String> addedClasses = new ArrayList<String>();
        if(replaceClasses != null) {
            Collection<String> importClasses = replaceClasses.values();
            for (String importClass : importClasses) {
                if (!importClass.startsWith("java.lang.") && !addedClasses.contains(importClass)
                        && !importClass.startsWith("[")) {
                    contents.add(String.format("import %s;", importClass));
                    addedClasses.add(importClass);
                }
            }
        }

        String beanName = beanName(table.getName(), ignorePrefix);

        contents.add("/**");
        contents.add("IService:");
        contents.add("");

        contents.addAll(generateInterface(beanName));
        contents.add("");

        contents.add("ServiceImplement:");
        contents.add("");

        contents.addAll(generateImplement(beanName));

        contents.add("*/");

        //add new line
        contents.add("");

        contents.add(String.format("@RdTable(name = \"%s\", comment = \"%s\")",
                table.getName().toUpperCase(Locale.ROOT),
                table.getComment() != null?table.getComment().trim():""));


        if(useExtendEntity){
            //public class Product extends BaseEntity {
            contents.add(String.format("public class %s extends BaseEntity {", beanName));
        }else{
            contents.add(String.format("public class %s implements Serializable {", beanName));
        }
        //add new line
        contents.add("");

        //serial version
        contents.add(FOUR_SPACE +  "private static final long serialVersionUID = 1L;");

        //add new line
        contents.add("");

        List<String> constantFields = new ArrayList<String>();
        List<String> fields = new ArrayList<String>();
        List<String> getSeters = new ArrayList<String>();
        boolean containsDate = false;
        boolean containColumnType = false;
        for(int i = 0; i < columns.size(); i++){
            TableColumn column = columns.get(i);
            String columnName = column.getColumn().toUpperCase(Locale.ROOT);
            String fieldName = QueryUtils.displayName(column.getColumn());
            String className = column.getColumnClass();
            if(replaceClasses != null && replaceClasses.containsKey(className)) {
                className = replaceClasses.get(className);
            }
            if("[B".equals(className)){
                className = "byte []";
            }
            boolean isTimestamp = (Long.class.getName().equals(className) || BigDecimal.class.getName().equals(className)) && column.getPrecision() == 15 && column.getScale() == 0;
            if(isTimestamp){
                className = Date.class.getName();
                if(!baseEntityInfo.contains(fieldName + "," + className)){
                    containsDate = true;
                }
            }
            if(!useExtendEntity || !baseEntityInfo.contains(fieldName + "," + className)){
                if(!ORMUtils.isEmpty(column.getComment())) {
                    constantFields.add(FOUR_SPACE + "/** " + column.getComment() + " */");
                }
                constantFields.add(FOUR_SPACE + String.format("public static final String T_%s = \"%s\";", columnName, columnName));

                List<String> rdColumnInfo = new ArrayList<String>();
                boolean skipDefaultValue = false;
                rdColumnInfo.add(String.format("name = T_%s", columnName));
                if(!ORMUtils.isEmpty(column.getComment())){
                    String [] cs = column.getComment().split(";");
                    if(cs.length > 0){
                        rdColumnInfo.add(String.format("title = \"%s\"", cs[0]));
                    }
                    if(cs.length > 1){
                        rdColumnInfo.add(String.format("description = \"%s\"", cs[1]));
                    }
                }
                if(String.class.getName().equals(className) || Character.class.getName().equals(className)){
                    if("text".equalsIgnoreCase(column.getType()) && column.getLength() == 65535){
                        rdColumnInfo.add("type = ColumnType.TEXT");
                        containColumnType = true;
                    }else if(column.getLength() != 191){
                        rdColumnInfo.add(String.format("length = %d", column.getLength()));
                    }else{
                        //do nothing.
                    }
                }else if((Integer.class.getName().equals(className) || Long.class.getName().equals(className))) {
                    if(column.getPrecision() > 0){
                        rdColumnInfo.add(String.format("precision = %d", column.getPrecision()));
                    }
                }else if(Double.class.getName().equals(className) || Float.class.getName().equals(className) || BigDecimal.class.getName().equals(className)){
                    if(column.getPrecision() > 0){
                        rdColumnInfo.add(String.format("precision = %d", column.getPrecision()));
                    }
                    if(column.getScale() > 0){
                        rdColumnInfo.add(String.format("scale = %d", column.getScale()));
                    }
                }else if(Date.class.getName().equals(className)){
                    if(isTimestamp) {
                        rdColumnInfo.add("type = ColumnType.LONG");
                        containColumnType = true;
                    }else if("datetime".equalsIgnoreCase(column.getType())){
                        rdColumnInfo.add("type = ColumnType.DATETIME");
                        containColumnType = true;
                    }else if("timestamp".equalsIgnoreCase(column.getType())){
                        rdColumnInfo.add("type = ColumnType.TIMESTAMP");
                        containColumnType = true;
                        skipDefaultValue = true;
                    }
                }else if("[B".equals(className) || "byte []".equals(className)){
                    if("blob".equals(column.getType())){
                        rdColumnInfo.add("type = ColumnType.BLOB");
                        containColumnType = true;
                    }else if("varbinary".equals(column.getType())){
                        rdColumnInfo.add("type = ColumnType.BINARY");
                        containColumnType = true;
                    }
                }
                if(!ORMUtils.isEmpty(column.getDefaultValue()) && !skipDefaultValue){
                    rdColumnInfo.add(String.format("defaultValue = \"%s\"", column.getDefaultValue()));
                }
                rdColumnInfo.add(String.format("order = %d", (i+1) * 10));
                int lastIndex = className.lastIndexOf(".");
                if(lastIndex > -1) {
                    className = className.substring(lastIndex + 1);
                }
                if(!column.isNullable()){
                    rdColumnInfo.add("nullable = false");
                }
                if(column.isPrimaryKey()) {
                    fields.add(FOUR_SPACE + String.format("@RdId"));
                }
                fields.add(FOUR_SPACE + String.format("@RdColumn(%s)", ORMUtils.join(rdColumnInfo, ", ")));
                fields.add(FOUR_SPACE +  String.format("private %s %s;", className, fieldName));
                fields.add("");
                getSeters.addAll(createGetAndSet(FOUR_SPACE, className, fieldName));

            }
        }
        if(containsDate){
            contents.add(insertIndex, "import java.util.Date;");
        }
        if (!containColumnType){
            contents.remove("import com.objectsql.support.ColumnType;");
        }
        contents.addAll(constantFields);

        //add new line
        contents.add("");

        contents.addAll(fields);

        contents.addAll(getSeters);

        contents.add("}");

        return ORMUtils.join(contents, "\n");
    }

    public static List<String> generateImplement(String beanName) {
        List<String> contents = new ArrayList<String>();

        contents.add(String.format("import %s;", BaseServiceImpl.class.getName()));
        contents.add(String.format("import %s;", ObjectSQLManager.class.getName()));

        //add new line
        contents.add("");

        contents.add(String.format("public class %sServiceImpl extends BaseServiceImpl<%s> implements I%sService {",
                beanName, beanName, beanName));
        contents.add(FOUR_SPACE + String.format("public %sServiceImpl(ObjectSQLManager objectSQLManager){", beanName));
        contents.add(FOUR_SPACE + FOUR_SPACE + "super(objectSQLManager);");
        contents.add(FOUR_SPACE + "}");
        contents.add("}");

        return contents;
    }

    public static List<String> generateInterface(String beanName) {
        List<String> contents = new ArrayList<String>();

        contents.add(String.format("import %s;", IBaseService.class.getName()));

        //add new line
        contents.add("");

        contents.add(String.format("public interface I%sService extends IBaseService<%s> {", beanName, beanName));
        contents.add("");
        contents.add("}");

        return contents;
    }

    private static List<String> getBaseEntityInfo(Class<?> clazz){
        List<String> info = new ArrayList<String>();
        if(clazz != null) {
            Field[] fields = clazz.getDeclaredFields();
            for (Field field : fields) {
                if (field.getDeclaredAnnotation(RdColumn.class) != null) {
                    info.add(field.getName() + "," + field.getType().getName());
                }
            }
        }
        return info;
    }

    public static String beanName(String tableName, boolean ignorePrefix){
        tableName = tableName.toUpperCase(Locale.ROOT);
        String [] bns =  tableName.split("_");
        StringBuffer contents = new StringBuffer();
        for(int i = 0; i < bns.length; i++){
            String bn = bns[i];
            if(!(i == 0 && ignorePrefix)){
                if (bn.length() == 1){
                    throw new RuntimeException("Column name length should more than 1.");
                }
                contents.append(bn.substring(0, 1) + bn.substring(1).toLowerCase(Locale.ROOT));
            }
        }
        return contents.toString();
    }

    public static List<String> createGetAndSet(String FOUR_SPACE, String className, String fieldName){
        List<String> temp = new ArrayList<String>();
        if(fieldName == null || fieldName.length() <= 1) {
            throw new RuntimeException("Field name length should more than 1.");
        }
        String suffix = fieldName.substring(0,1).toUpperCase(Locale.ROOT) + fieldName.substring(1);
        //add new line
        temp.add("");
        temp.add(FOUR_SPACE + String.format("public %s get%s(){", className, suffix));
        temp.add(FOUR_SPACE + FOUR_SPACE + String.format("return this.%s;", fieldName));
        temp.add(FOUR_SPACE + "}");
        temp.add(FOUR_SPACE + String.format("public void set%s(%s %s){", suffix, className, fieldName));
        temp.add(FOUR_SPACE + FOUR_SPACE + String.format("this.%s = %s;", fieldName, fieldName));
        temp.add(FOUR_SPACE + "}");
        return temp;
    }

    private static boolean contains(List<TableColumn> columns, Map<String, String> replace, List<String> values){
        if (replace == null){
            return false;
        }
        List<String> columnClass = new ArrayList<String>();
        for(TableColumn column : columns){
            String clazz = column.getColumnClass();
            if(BigDecimal.class.getName().equals(clazz) || Long.class.getName().equals(clazz)){
                if(column.getPrecision() == 15 && column.getScale() == 0){
                    clazz = Date.class.getName();
                }
            }
            if(replace.containsKey(clazz)) {
                columnClass.add(QueryUtils.displayName(column.getColumn()) + "," + replace.get(clazz));
            }else{
                columnClass.add(QueryUtils.displayName(column.getColumn()) + "," + clazz);
            }
        }
        return columnClass.containsAll(values);
    }

}