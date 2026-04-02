package com.experlog.zql;

import java.util.Hashtable;

//PY.Gibello 21 Apr 2001 - New class

public class ZUtils {

  private static Hashtable<String,Integer> fcts_ = null;

  public static final int VARIABLE_PLIST = 10000;

  public static void addCustomFunction(String fct, int nparm) {
    if(fcts_ == null) fcts_ = new Hashtable<String,Integer>();
    if(nparm <= 0) nparm = 1;
    fcts_.put(fct.toUpperCase(), new Integer(nparm));
  }

  public static int isCustomFunction(String fct) {
    Integer nparm;
    if(fct == null || fct.length()<1 || fcts_ == null
      || (nparm = (Integer)fcts_.get(fct.toUpperCase())) == null)
       return -1;
    return nparm.intValue();
  }

  public static boolean isAggregate(String op) {
    op = op.toUpperCase().trim();
    return op.equals("SUM") || op.equals("AVG")
        || op.equals("MAX") || op.equals("MIN")
        || op.equals("COUNT") || (fcts_ != null && fcts_.get(op) != null);
  }

  public static boolean isAggregate(int code) {
    return code == ZExpression.SUM || code == ZExpression.AVG
        || code == ZExpression.MAX || code == ZExpression.MIN
        || code == ZExpression.COUNT;
  }

  public static String getAggregateCall(String c) {
    int pos = c.indexOf('(');
    if(pos <= 0) return null;
    String call = c.substring(0,pos);
    if(ZUtils.isAggregate(call)) return call.trim();
    else return null;
  }

};

