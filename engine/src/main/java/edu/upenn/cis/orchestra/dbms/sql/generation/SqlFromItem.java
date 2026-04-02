package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZExp;
import com.experlog.zql.ZFromItem;
import com.experlog.zql.ZQuery;


/**
 * SqlFromItem: an SQL FROM clause (example: the FROM part of a SELECT...FROM).
 * @author gkarvoun
 * 
 */
public class SqlFromItem extends ZFromItem {

	public SqlFromItem() { super(); }

	public SqlFromItem(String fullname) {
		super(fullname);
	}
  
  public SqlFromItem(String alias, ZQuery subquery) {
      super(alias, subquery);
  }
  
  public SqlFromItem(ZFromItem.Join type, ZFromItem left, ZFromItem right, ZExp cond) {
     super(type, left, right, cond);
  }
};
