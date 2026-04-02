package com.experlog.zql;


/**
 * ZSelectItem: an item in the SELECT part of an SQL query.
 * (The SELECT part of a query is a Vector of ZSelectItem).
 */
public class ZSelectItem extends ZAliasedName {

  ZExp expression_ = null;
  String aggregate_ = null;

  /**
   * Create a new SELECT item
   */
  public ZSelectItem() { super(); }

  /**
   * Create a new SELECT item, given its name (for column names and wildcards).
   * @param fullname A string that represents a column name or wildcard
   * (example: a.*).
   */
  public ZSelectItem(String fullname) {
    super(fullname, ZAliasedName.FORM_COLUMN);
    setAggregate(ZUtils.getAggregateCall(fullname)); //PY.Gibello 21 Apr 2001
  }

  /**
   * @return An SQL Expression if this SELECT item is an expression,
   * a ZConstant if it is a column name,  null if it is a wildcard
   */
  public ZExp getExpression() {
    if(isExpression()) return expression_;
    else if(isWildcard()) return null;
    else {
      return new ZConstant(getColumn(), ZConstant.COLUMNNAME);
    }
  }

  /**
   * Initialize this SELECT item as an SQL expression (not a column name
   * nor wildcard)
   * Example: SELECT a+b FROM table1; (a+b is an expression)
   */
  public void setExpression(ZExp e) {
    expression_ = e;
    strform_ = expression_.toString();
  }

  /**
   * @return true if this item is an SQL expression, false if not.
   * (Example: SELECT a+b, c FROM num; -> a+b is an expression, not c)
   */
  public boolean isExpression() { return (expression_ != null); }

  /**
   * Initialize an aggregate function on this item
   * (generally SUM, AVG, MAX, MIN)
   * Example: SELECT AVG(age) FROM people; -> The aggregate function is AVG.
   * @param a The name of the aggregate function
   * (a String, like SUM, AVG, MAX, MIN)
   */
  public void setAggregate(String a) { aggregate_ = a; }

  /**
   * If this item is an aggregate function, return the function name.
   * @return The name of an aggregate function (generally SUM, AVG, MAX, MIN),
   * or null if there's no aggregate.
   * Example: SELECT name, AVG(age) FROM people; -> null for the "name" item,
   * and "AVG" for the "AVG(age)" item.
   */
  public String getAggregate() { return aggregate_; }

  public String toString() {
    if (aggregate_ == null) {
        return super.toString();
    } else {
        return aggregate_ + "(" + strform_ + ") " + alias_;
    }
  }
};

