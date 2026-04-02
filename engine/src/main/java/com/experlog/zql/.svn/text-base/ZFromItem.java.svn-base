package com.experlog.zql;


/**
 * ZFromItem: an SQL FROM clause (example: the FROM part of a SELECT...FROM).
 */
public class ZFromItem extends ZAliasedName {

  /**
   * Create a new FROM clause.
   * See the ZAliasedName constructor for more information.
   */
  public ZFromItem() { super(); }

  /**
   * Create a new FROM clause on a given table.
   * See the ZAliasedName constructor for more information.
   * @param fullname the table name.
   */
  public ZFromItem(String fullname) {
    super(fullname, ZAliasedName.FORM_TABLE);
  }
  
  public ZFromItem(String alias, ZQuery subquery) {
      super("CRAPFACE", ZAliasedName.FORM_TABLE);
      alias_ = alias;
      subquery_ = subquery;
  }
  
  public ZFromItem(Join type, ZFromItem left, ZFromItem right, ZExp cond) {
      join_ = type;
      left_ = left;
      right_ = right;
      cond_ = cond;
  }

  public String toString() {
      if (subquery_ != null) {
          return "(" + subquery_.toString() + ") " + alias_;
      } else if (join_ != Join.NONE) {
          String str = join2str(join_);
          return left_.toString() + " " + str + " " + right_.toString() + " ON " + cond_.toString(); 
      } else {
          return super.toString();
      }
  }

  protected static String join2str(Join j) {
      switch (j) {
      case INNERJOIN:       return "JOIN";
      case NATURALJOIN:     return "NATURAL JOIN";
      case LEFTOUTERJOIN:   return "LEFT OUTER JOIN";
      case RIGHTOUTERJOIN:  return "RIGHT OUTER JOIN";
      case FULLOUTERJOIN:   return "FULL OUTER JOIN";
      default:              return null;
      }
  }
  
  public enum Join {
      NONE,
      INNERJOIN,
      NATURALJOIN,
      LEFTOUTERJOIN,
      RIGHTOUTERJOIN,
      FULLOUTERJOIN
  };

  ZQuery subquery_ = null;
  Join join_ = Join.NONE;
  ZFromItem left_ = null;
  ZFromItem right_ = null;
  ZExp cond_ = null;
};

