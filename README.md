# Light ORM by ursful.com
<br/>
--------------------save 保存----------------------<br/>
 RoseType roseType = new RoseType();<br/>
 roseType.setId("ID");<br/>
 roseType.setName("Rose Type");<br/>
 roseType.setOrderNum(1);<br/>
 roseTypeService.save(roseType);<br/>
<br/>
--------------------update 更新----------------------<br/>
 RoseType roseType = new RoseType();<br/>
 roseType.setId("ID");<br/>
 roseType.setOrderNum(2);<br/>
 roseTypeService.update(roseType);<br/>
<br/>
--------------------delete 删除----------------------<br/>
 RoseType roseType = new RoseType();<br/>
 roseType.setId("ID");<br/>
 roseTypeService.delete(roseType);<br/>
 or<br/>
 roseTypeService.delete("ID");<br/>

--------------------list 列表----------------------<br/>
 List<Rose> list = roseService.list();<br/>
<br/>
 List<Rose> list = roseService.list(new Express(Rose.T_ORDER_NUM, 3, ExpressionType.CDT_Equal));<br/>
<br/>
 List<Rose> list = roseService.list(new Express(Rose.T_ORDER_NUM, 3, ExpressionType.CDT_Less),<br/>
                new Express(Rose.T_ORDER_NUM, 0, ExpressionType.CDT_More));<br/>
 List<Rose> list = roseService.list(1, 5);<br/>
<br/>
 Terms terms = new Terms();<br/>
 terms.and(new Express(Rose.T_ORDER_NUM, 2, ExpressionType.CDT_MoreEqual))<br/>
      .or(new Express(Rose.T_ORDER_NUM, 6, ExpressionType.CDT_Less));<br/>
<br/>
 List<Rose> list = roseService.list(terms);<br/>
<br/>
 List<Rose> list = roseService.list(terms, new MultiOrder().asc(Rose.T_ORDER_NUM));<br/>
<br/>
--------------------base Query 简单查询------------------------------<br/>
 IBaseQuery query = new BaseQueryImpl();<br/>
 query.table(RoseType.class);<br/>
 query.where(Rose.T_ORDER_NUM, 2, ExpressionType.CDT_MoreEqual);<br/>
<br/>
 List<Rose> roses = roseService.query(query);<br/>
<br/>
 List<Rose> roses = roseService.query(query, 2);<br/>
<br/>
  int c = roseService.queryCount(query);<br/>
<br/>
  Page page = new Page();<br/>
  page.setPage(1);<br/>
  page.setSize(2);<br/>
  page = roseService.queryPage(query, page);<br/>
<br/>
--------------------multi Query 复杂查询------------------------------<br/>
  IMultiQuery query = new MultiQueryImpl();<br/>
<br/>
  AliasTable arose = query.table(Rose.class);<br/>
  AliasTable aroseType = query.table(RoseType.class);<br/>
<br/>
  query.where(arose.c(Rose.T_TYPE), aroseType.c(RoseType.T_ID));<br/>
  query.where(arose.c(Rose.T_ORDER_NUM), 2, ExpressionType.CDT_MoreEqual);<br/>
  query.createQuery(Map.class, arose.c(Expression.EXPRESSION_ALL), aroseType.c(RoseType.T_NAME, "TYPE_NAME"));<br/>
<br/>
  List<Map<String, Object>> roses = roseService.query(query);<br/>
  for(Map<String, Object> rose : roses) {<br/>
      System.out.println(rose.get("typeName"));<br/>
  }<br/>
<br/>
--------------------listener 监听器------------------------------<br/>
 IDefaultListener/IORMListener/IChangeListener<br/>