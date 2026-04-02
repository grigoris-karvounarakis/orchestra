package com.experlog.zql;

import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
 * ZUpdate: an SQL UPDATE statement.
 */
public class ZUpdate implements ZStatement {

  String table_;
  String alias_;
  Hashtable<String,ZExp> set_;
  ZExp where_ = null;
  Vector<String> columns_ = null;
  ZExp query_ = null;
  
  /**
   * Create an UPDATE statement on a given table.
   */
  public ZUpdate(String tab) {
    table_ = new String(tab);
  }

  public String getTable() {
    return table_;
  }

  /**
   * Insert a SET... clause in the UPDATE statement
   * @param t A Hashtable, where keys are column names (the columns to update),
   * and values are ZExp objects (the column values).
   * For example, the values may be ZConstant objects (like "Smith") or
   * more complex SQL Expressions.
   */
  public void addSet(Hashtable<String,ZExp> t) {
    set_ = t;
  }
  
  // add query to be rhs of assignment
  public void addQuery(ZExp q) {
      query_ = q;
  }
  
  public ZExp getQuery() {
      return query_;
  }
  
  public void setColumns(Vector<String> cols) {
      columns_ = cols;
  }
  
  public Vector<String> getColumns() {
      return columns_;
  }

  /**
   * Get the whole SET... clause
   * @return A Hashtable, where keys are column names (the columns to update),
   * and values are ZExp objects (Expressions that specify column values: for
   * example, ZConstant objects like "Smith").
   */
  public Hashtable getSet() { return set_; }

  public String getAlias() { return alias_; }
  
  public void setAlias(String alias) { alias_ = alias; }
  
  /**
   * Add one column=value pair to the SET... clause
   * This method also keeps track of the column order
   * @param col The column name
   * @param val The column value
   */
  public void addColumnUpdate(String col, ZExp val) {
    if(set_ == null) set_ = new Hashtable<String,ZExp>();
    set_.put(col, val);
    if(columns_ == null) columns_ = new Vector<String>();
    columns_.addElement(col);
  }

  /**
   * Get the SQL expression that specifies a given column's update value.
   * (for example, a ZConstant object like "Smith").
   * @param col The column name.
   * @return a ZExp, like a ZConstant representing a value, or a more complex
   * SQL expression.
   */
  public ZExp getColumnUpdate(String col) { return (ZExp)set_.get(col); }

  /**
   * Get the SQL expression that specifies a given column's update value.
   * (for example, a ZConstant object like "Smith").<br>
   * WARNING: This method will work only if column/value pairs have been
   * inserted using addColumnUpdate() - otherwise it is not possible to guess
   * what the right order is, and null will be returned.
   * @param num The column index (starting from 1).
   * @return a ZExp, like a ZConstant representing a value, or a more complex
   * SQL expression.
   */
  public ZExp getColumnUpdate(int index) {
    if(--index < 0) return null;
    if(columns_ == null || index >= columns_.size()) return null;
    String col = (String)columns_.elementAt(index);
    return (ZExp)set_.get(col);
  }

  /**
   * Get the column name that corresponds to a given index.<br>
   * WARNING: This method will work only if column/value pairs have been
   * inserted using addColumnUpdate() - otherwise it is not possible to guess
   * what the right order is, and null will be returned.
   * @param num The column index (starting from 1).
   * @return The corresponding column name.
   */
  public String getColumnUpdateName(int index) {
    if(--index < 0) return null;
    if(columns_ == null || index >= columns_.size()) return null;
    return (String)columns_.elementAt(index);
  }

  /**
   * Returns the number of column/value pairs in the SET... clause.
   */
  public int getColumnUpdateCount() {
    if(set_ == null) return 0;
    return set_.size();
  }

  /**
   * Insert a WHERE... clause in the UPDATE statement
   * @param w An SQL Expression compatible with a WHERE... clause.
   */
  public void addWhere(ZExp w) { where_ = w; }

  /**
   * Get the WHERE clause of this UPDATE statement.
   * @return An SQL Expression compatible with a WHERE... clause.
   */
  public ZExp getWhere() { return where_; }

  public String toString() {
    StringBuffer buf = new StringBuffer("update " + table_);
    if (alias_ != null) {
        buf.append(" ");
        buf.append(alias_);
    }
    buf.append(" set ");

    if (query_ != null) {
        buf.append("(");
        boolean first = true;
        for (String s : columns_) {
            if (first) { first = false; } else { buf.append(", "); }
            buf.append(s);
        }
        buf.append(") = (");
        buf.append(query_.toString());
        buf.append(")");
    } else {
        Enumeration<String> e;
        if(columns_ != null) e = columns_.elements();
        else e = set_.keys();
        boolean first = true;
        while(e.hasMoreElements()) {
          String key = e.nextElement().toString();
          if(!first) buf.append(", ");
          buf.append(key + "=" + set_.get(key).toString()); 
          first = false;
        }
    }

    if(where_ != null) buf.append(" where " + where_.toString());
    return buf.toString();
  }
};

