package com.experlog.zql;


/**
 * ZTransactStmt: an SQL statement that concerns database transactions
 * (example: COMMIT, ROLLBACK, SET TRANSACTION)
 */
public class ZTransactStmt implements ZStatement {

  String statement_;
  String comment_ = null;
  boolean readOnly_ = false;

  public ZTransactStmt(String st) { statement_ = new String(st); }

  public void setComment(String c) { comment_ = new String(c); }
  public String getComment() { return comment_; }
  public boolean isReadOnly() { return readOnly_; }
};

