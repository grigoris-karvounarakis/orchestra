package com.experlog.zql;


/**
 * ZConstant: a representation of SQL constants
 */
public class ZConstant implements ZExp {

  /**
   * ZConstant types
   */
  public static final int UNKNOWN = -1;
  public static final int COLUMNNAME = 0;
  public static final int NULL = 1;
  public static final int NUMBER = 2;
  public static final int STRING = 3;
  public static final int LABELEDNULL = 4;
  public static final int DATE = 5;

  int type_ = ZConstant.UNKNOWN;
  String val_ = null;

  /**
   * Create a new constant, given its name and type.
   */
  public ZConstant(String v, int typ) {
    val_ = new String(v);
    type_ = typ;
  }

  /*
   * @return the constant value
   */
  public String getValue() { return val_; }

  /*
   * @return the constant type
   */
  public int getType() { return type_; }

  public String toString() {
    if (type_ == STRING) {
        return '\'' + val_ + '\'';
    } else if (type_ == LABELEDNULL) {
        return "null(\'" + val_ + "\')";
    } else if (type_ == DATE) {
        return "date \'" + val_ + "\'";
    } else {
        return val_;
    }
  }
};

