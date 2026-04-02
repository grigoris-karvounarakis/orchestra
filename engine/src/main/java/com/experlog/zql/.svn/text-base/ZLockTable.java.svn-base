package com.experlog.zql;

import java.util.Vector;

/**
 * ZLockTable: an SQL LOCK TABLE statement
 */
public class ZLockTable implements ZStatement {

  boolean nowait_ = false;
  String lockMode_ = null;
  Vector tables_ = null;

  public ZLockTable() {}

  public void addTables(Vector v) { tables_ = v; }
  public Vector getTables() { return tables_; } 
  public void setLockMode(String lc) { lockMode_ = new String(lc); }
  public String getLockMode() { return lockMode_; }
  public boolean isNowait() { return nowait_; }
};

