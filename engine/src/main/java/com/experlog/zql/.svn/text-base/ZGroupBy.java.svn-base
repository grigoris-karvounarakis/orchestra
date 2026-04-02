package com.experlog.zql;

import java.util.Vector;

/**
 * ZGroupBy: an SQL GROUP BY...HAVING clause
 */
public class ZGroupBy {

  Vector<ZExp> groupby_;
  ZExp having_ = null;

  /**
   * Create a GROUP BY given a set of Expressions
   * @param exps A vector of SQL Expressions (ZExp objects).
   */
  public ZGroupBy(Vector<ZExp> exps) { groupby_ = exps; }

  /**
   * Initiallize the HAVING part of the GROUP BY
   * @param e An SQL Expression (the HAVING clause)
   */
  public void setHaving(ZExp e) { having_ = e; }

  /**
   * Get the GROUP BY expressions
   * @return A vector of SQL Expressions (ZExp objects)
   */
  public Vector<ZExp> getGroupBy() { return groupby_; }

  /**
   * Get the HAVING clause
   * @return An SQL expression
   */
  public ZExp getHaving() { return having_; }

  public String toString() {
    StringBuffer buf = new StringBuffer("group by ");

    //buf.append(groupby_.toString());
    buf.append(groupby_.elementAt(0).toString());
    for(int i=1; i<groupby_.size(); i++) {
      buf.append(", " + groupby_.elementAt(i).toString());
    }
    if(having_ != null) {
      buf.append(" having " + having_.toString());
    }
    return buf.toString();
  }
};

