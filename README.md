# Light ORM by ursful.com

--------------------save 保存----------------------
 RoseType roseType = new RoseType();
 roseType.setId("ID");
 roseType.setName("Rose Type");
 roseType.setOrderNum(1);
 roseTypeService.save(roseType);

--------------------update 更新----------------------
 RoseType roseType = new RoseType();
 roseType.setId("ID");
 roseType.setOrderNum(2);
 roseTypeService.update(roseType);

--------------------delete 删除----------------------
 RoseType roseType = new RoseType();
 roseType.setId("ID");
 roseTypeService.delete(roseType);
 or
 roseTypeService.delete("ID");

--------------------list 列表----------------------
 List<Rose> list = roseService.list();

 List<Rose> list = roseService.list(new Express(Rose.T_ORDER_NUM, 3, ExpressionType.CDT_Equal));

 List<Rose> list = roseService.list(new Express(Rose.T_ORDER_NUM, 3, ExpressionType.CDT_Less),
                new Express(Rose.T_ORDER_NUM, 0, ExpressionType.CDT_More));
 List<Rose> list = roseService.list(1, 5);

 Terms terms = new Terms();
 terms.and(new Express(Rose.T_ORDER_NUM, 2, ExpressionType.CDT_MoreEqual))
      .or(new Express(Rose.T_ORDER_NUM, 6, ExpressionType.CDT_Less));

 List<Rose> list = roseService.list(terms);

 List<Rose> list = roseService.list(terms, new MultiOrder().asc(Rose.T_ORDER_NUM));

--------------------base Query 简单查询------------------------------
 IBaseQuery query = new BaseQueryImpl();
 query.table(RoseType.class);
 query.where(Rose.T_ORDER_NUM, 2, ExpressionType.CDT_MoreEqual);

 List<Rose> roses = roseService.query(query);

 List<Rose> roses = roseService.query(query, 2);

  int c = roseService.queryCount(query);

  Page page = new Page();
  page.setPage(1);
  page.setSize(2);
  page = roseService.queryPage(query, page);

--------------------multi Query 复杂查询------------------------------
  IMultiQuery query = new MultiQueryImpl();

  AliasTable arose = query.table(Rose.class);
  AliasTable aroseType = query.table(RoseType.class);

  query.where(arose.c(Rose.T_TYPE), aroseType.c(RoseType.T_ID));
  query.where(arose.c(Rose.T_ORDER_NUM), 2, ExpressionType.CDT_MoreEqual);
  query.createQuery(Map.class, arose.c(Expression.EXPRESSION_ALL), aroseType.c(RoseType.T_NAME, "TYPE_NAME"));

  List<Map<String, Object>> roses = roseService.query(query);
  for(Map<String, Object> rose : roses) {
      System.out.println(rose.get("typeName"));
  }

--------------------listener 监听器------------------------------
 IDefaultListener/IORMListener/IChangeListener