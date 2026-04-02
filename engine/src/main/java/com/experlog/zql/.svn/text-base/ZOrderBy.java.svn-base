package com.experlog.zql;

/**
 * An SQL query ORDER BY clause.
 */
public class ZOrderBy {
  ZExp exp_;
  boolean asc_ = true;

  public ZOrderBy(ZExp e) { exp_ = e; }

  /**
   * Set the order to ascending or descending (defailt is ascending order).
   * @param a true for ascending order, false for descending order.
   */
  public void setAscOrder(boolean a) { asc_ = a; }

  /**
   * Get the order (ascending or descending)
   * @return true if ascending order, false if descending order.
   */
  public boolean getAscOrder() { return asc_; }

  /**
   * Get the ORDER BY expression.
   * @return An expression (generally, a ZConstant that represents a column
   * name).
   */
  public ZExp getExpression() { return exp_; }

  public String toString() {
    return exp_.toString() + " " + (asc_ ? "ASC" : "DESC");
  }
};

