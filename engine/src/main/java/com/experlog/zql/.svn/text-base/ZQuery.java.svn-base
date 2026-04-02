package com.experlog.zql;

import java.util.Vector;

/**
 * ZQuery: an SQL SELECT statement
 */
public class ZQuery implements ZStatement, ZExp {

  protected Vector<ZSelectItem> select_;
  protected boolean distinct_ = false;
  protected Vector<ZFromItem> from_;
  protected ZExp where_ = null;
  protected ZGroupBy groupby_ = null;
  protected ZExpression setclause_ = null;
  protected Vector<ZOrderBy> orderby_ = null;
  protected boolean forupdate_ = false;

  /**
   * Create a new SELECT statement
   */
  public ZQuery() {}

  /**
   * Insert the SELECT part of the statement
   * @param s A vector of ZSelectItem objects
   */
  public void addSelect(Vector<ZSelectItem> s) { select_ = s; }

  /**
   * Insert the FROM part of the statement
   * @param f a Vector of ZFromItem objects
   */
  public void addFrom(Vector<ZFromItem> f) { from_ = f; }

  /**
   * Insert a WHERE clause
   * @param w An SQL Expression
   */
  public void addWhere(ZExp w) { where_ = w; }

  /**
   * Insert a GROUP BY...HAVING clause
   * @param g A GROUP BY...HAVING clause
   */
  public void addGroupBy(ZGroupBy g) { groupby_ = g; }

  /**
   * Insert a SET clause (generally UNION, INTERSECT or MINUS)
   * @param s An SQL Expression (generally UNION, INTERSECT or MINUS)
   */
  public void addSet(ZExpression s) { setclause_ = s; }

  /**
   * Insert an ORDER BY clause
   * @param v A vector of ZOrderBy objects
   */
  public void addOrderBy(Vector<ZOrderBy> v) { orderby_ = v; }

  /**
   * Get the SELECT part of the statement
   * @return A vector of ZSelectItem objects
   */
  public Vector<ZSelectItem> getSelect() { return select_; }

  /**
   * Get the FROM part of the statement
   * @return A vector of ZFromItem objects
   */
  public Vector<ZFromItem> getFrom() { return from_; }

  /**
   * Get the WHERE part of the statement
   * @return An SQL Expression or sub-query (ZExpression or ZQuery object)
   */
  public ZExp getWhere() { return where_; }

  /**
   * Get the GROUP BY...HAVING part of the statement
   * @return A GROUP BY...HAVING clause
   */
  public ZGroupBy getGroupBy() { return groupby_; }

  /**
   * Get the SET clause (generally UNION, INTERSECT or MINUS)
   * @return An SQL Expression (generally UNION, INTERSECT or MINUS) 
   */
  public ZExpression getSet() { return setclause_; }

  /**
   * Get the ORDER BY clause
   * @param v A vector of ZOrderBy objects
   */
  public Vector getOrderBy() { return orderby_; }

  /**
   * @return true if it is a SELECT DISTINCT query, false otherwise.
   */
  public boolean isDistinct() { return distinct_; }
  
  public void setDistinct(boolean distinct){
	  distinct_ = distinct;
  }

  /**
   * @return true if it is a FOR UPDATE query, false otherwise.
   */
  public boolean isForUpdate() { return forupdate_; }


  public String toString() {
    StringBuffer buf = new StringBuffer("select ");

    if(distinct_) buf.append("distinct ");

    //buf.append(select_.toString());
    int i;
    buf.append(select_.elementAt(0).toString());
    for(i=1; i<select_.size(); i++) {
      buf.append(", " + select_.elementAt(i).toString());
    }

    //buf.append(" from " + from_.toString());
    if (from_.size() > 0) {
	    buf.append(" from ");
	    buf.append(from_.elementAt(0).toString());
	    for(i=1; i<from_.size(); i++) {
	      buf.append(", " + from_.elementAt(i).toString());
	    }
    }     	

    if(where_ != null) {
      buf.append(" where " + where_.toString());
    }
    if(groupby_ != null) {
      buf.append(" " + groupby_.toString());
    }
    if(setclause_ != null) {
      buf.append(" " + setclause_.toString());
    }
    if(orderby_ != null) {
      buf.append(" order by ");
      //buf.append(orderby_.toString());
      buf.append(orderby_.elementAt(0).toString());
      for(i=1; i<orderby_.size(); i++) {
        buf.append(", " + orderby_.elementAt(i).toString());
      }
    }
    
    if(forupdate_) buf.append(" for update");

    return buf.toString();
  }

};

