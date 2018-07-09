# Light ORM by ursful.com

--------------------save 保存----------------------
 RoseType roseType = new RoseType();
 roseType.setId("ID");
 roseType.setName("Rose Type");
 roseType.setOrderNum(1);
 roseTypeService.save(roseType);

--------------------update 更新----------------------\r
 RoseType roseType = new RoseType();\r
 roseType.setId("ID");\r
 roseType.setOrderNum(2);\r
 roseTypeService.update(roseType);\r
\r
--------------------delete 删除----------------------\r
 RoseType roseType = new RoseType();\r
 roseType.setId("ID");\r
 roseTypeService.delete(roseType);\r
 or\r
 roseTypeService.delete("ID");\r

--------------------list 列表----------------------\r
 List<Rose> list = roseService.list();\r
\r
 List<Rose> list = roseService.list(new Express(Rose.T_ORDER_NUM, 3, ExpressionType.CDT_Equal));\r
\r
 List<Rose> list = roseService.list(new Express(Rose.T_ORDER_NUM, 3, ExpressionType.CDT_Less),\r
                new Express(Rose.T_ORDER_NUM, 0, ExpressionType.CDT_More));\r
 List<Rose> list = roseService.list(1, 5);\r
\r
 Terms terms = new Terms();\r
 terms.and(new Express(Rose.T_ORDER_NUM, 2, ExpressionType.CDT_MoreEqual))\r
      .or(new Express(Rose.T_ORDER_NUM, 6, ExpressionType.CDT_Less));\r
\r
 List<Rose> list = roseService.list(terms);\r
\r
 List<Rose> list = roseService.list(terms, new MultiOrder().asc(Rose.T_ORDER_NUM));\r
\r
--------------------base Query 简单查询------------------------------\r
 IBaseQuery query = new BaseQueryImpl();\r
 query.table(RoseType.class);\r
 query.where(Rose.T_ORDER_NUM, 2, ExpressionType.CDT_MoreEqual);\r
\r
 List<Rose> roses = roseService.query(query);\r
\r
 List<Rose> roses = roseService.query(query, 2);\r
\r
  int c = roseService.queryCount(query);\r
\r
  Page page = new Page();\r
  page.setPage(1);\r
  page.setSize(2);\r
  page = roseService.queryPage(query, page);\r
\r
--------------------multi Query 复杂查询------------------------------\r
  IMultiQuery query = new MultiQueryImpl();\r
\r
  AliasTable arose = query.table(Rose.class);\r
  AliasTable aroseType = query.table(RoseType.class);\r
\r
  query.where(arose.c(Rose.T_TYPE), aroseType.c(RoseType.T_ID));\r
  query.where(arose.c(Rose.T_ORDER_NUM), 2, ExpressionType.CDT_MoreEqual);\r
  query.createQuery(Map.class, arose.c(Expression.EXPRESSION_ALL), aroseType.c(RoseType.T_NAME, "TYPE_NAME"));\r
\r
  List<Map<String, Object>> roses = roseService.query(query);\r
  for(Map<String, Object> rose : roses) {\r
      System.out.println(rose.get("typeName"));\r
  }\r
\r
--------------------listener 监听器------------------------------\r
 IDefaultListener/IORMListener/IChangeListener\r