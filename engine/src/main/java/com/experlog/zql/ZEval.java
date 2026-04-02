package com.experlog.zql;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.Vector;

/**
 * Evaluate SQL expressions
 */
public class ZEval {

  /**
   * Evaluate a boolean expression to true or false (for example, SQL WHERE
   * clauses are boolean expressions)
   * @param tuple The tuple on which to evaluate the expression
   * @param exp The expression to evaluate
   * @return true if the expression evaluate to true for this tuple,
   * false if not.
   */
  public boolean eval(ZTuple tuple, ZExp exp) throws SQLException {

    if(tuple == null || exp == null)  {
      throw new SQLException("ZEval.eval(): null argument or operator");
    }
    if(! (exp instanceof ZExpression))
      throw new SQLException("ZEval.eval(): only expressions are supported");

    ZExpression pred = (ZExpression)exp;
    String op = pred.getOperator();

    if(op.equals("AND")) {
      boolean and = true;
      for(int i = 0; i<pred.nbOperands(); i++) {
        and &= eval(tuple, pred.getOperand(i));
      }
      return and;
    } else if(op.equals("OR")) {
      boolean or = false;
      for(int i = 0; i<pred.nbOperands(); i++) {
        or |= eval(tuple, pred.getOperand(i));
      }
      return or;
    } else if(op.equals("NOT")) {
      return ! eval(tuple, pred.getOperand(0));

    } else if(op.equals("=")) {
      return evalCmp(tuple, pred.getOperands()) == 0;
    } else if(op.equals("!=")) {
      return evalCmp(tuple, pred.getOperands()) != 0;
    } else if(op.equals("<>")) {
      return evalCmp(tuple, pred.getOperands()) != 0;
    } else if(op.equals("#")) {
      throw new SQLException("ZEval.eval(): Operator # not supported");
    } else if(op.equals(">")) {
      return evalCmp(tuple, pred.getOperands()) > 0;
    } else if(op.equals(">=")) {
      return evalCmp(tuple, pred.getOperands()) >= 0;
    } else if(op.equals("<")) {
      return evalCmp(tuple, pred.getOperands()) < 0;
    } else if(op.equals("<=")) {
      return evalCmp(tuple, pred.getOperands()) <= 0;

    } else if(op.equals("BETWEEN") || op.equals("NOT BETWEEN")) {

      // Between: borders included
      ZExpression newexp = new ZExpression(ZExpression.AND, 
        new ZExpression(ZExpression.GTE, pred.getOperand(0), pred.getOperand(1)),
        new ZExpression(ZExpression.LTE, pred.getOperand(0), pred.getOperand(2)));

      if(op.equals("NOT BETWEEN"))
        return ! eval(tuple, newexp);
      else
        return eval(tuple, newexp);

    } else if(op.equals("LIKE") || op.equals("NOT LIKE")) {
      throw new SQLException("ZEval.eval(): Operator (NOT) LIKE not supported");

    } else if(op.equals("IN") || op.equals("NOT IN")) {

      ZExpression newexp = new ZExpression(ZExpression.OR);

      for(int i = 1; i < pred.nbOperands(); i++) {
        newexp.addOperand(new ZExpression(ZExpression.EQ,
          pred.getOperand(0), pred.getOperand(i)));
      }

      if(op.equals("NOT IN"))
        return ! eval(tuple, newexp);
      else
        return eval(tuple, newexp);

    } else if(op.equals("IS NULL")) {

      if(pred.nbOperands() <= 0 || pred.getOperand(0) == null) return true;
      ZExp x = pred.getOperand(0);
      if(x instanceof ZConstant) {
        return (((ZConstant)x).getType() == ZConstant.NULL);
      } else {
        throw new SQLException("ZEval.eval(): can't eval IS (NOT) NULL");
      }

    } else if(op.equals("IS NOT NULL")) {

      ZExpression x = new ZExpression(ZExpression.IS_NULL);
      x.setOperands(pred.getOperands());
      return ! eval(tuple, x);

    } else {
      throw new SQLException("ZEval.eval(): Unknown operator " + op);
    }

  }

  double evalCmp(ZTuple tuple, Vector operands) throws SQLException {

    if(operands.size() < 2) {
      throw new SQLException(
        "ZEval.evalCmp(): Trying to compare less than two values");
    }
    if(operands.size() > 2) {
      throw new SQLException(
        "ZEval.evalCmp(): Trying to compare more than two values");
    }

    Object o1 = null, o2 = null;

    o1 = evalExpValue(tuple, (ZExp)operands.elementAt(0));
    o2 = evalExpValue(tuple, (ZExp)operands.elementAt(1));

    if(o1 instanceof String || o2 instanceof String) {
      return(o1.equals(o2) ? 0 : -1);
    }

    if(o1 instanceof Number && o2 instanceof Number) {
      return ((Number)o1).doubleValue() - ((Number)o2).doubleValue();
    } else {
      throw new SQLException("ZEval.evalCmp(): can't compare (" + o1.toString()
        + ") with (" + o2.toString() + ")");
    }
  }

  double evalNumericExp(ZTuple tuple, ZExpression exp)
  throws SQLException {

    if(tuple == null || exp == null || exp.getOperator() == null)  {
      throw new SQLException("ZEval.eval(): null argument or operator");
    }

    String op = exp.getOperator();

    Object o1 = evalExpValue(tuple, (ZExp)exp.getOperand(0));
    if(! (o1 instanceof Double))
      throw new SQLException("ZEval.evalNumericExp(): expression not numeric");
    Double dobj = (Double)o1;

    if(op.equals("+")) {

      double val = dobj.doubleValue();
      for(int i = 1; i < exp.nbOperands(); i++) {
        Object obj = evalExpValue(tuple, (ZExp)exp.getOperand(i));
        val += ((Number)obj).doubleValue();
      }
      return val;

    } else if(op.equals("-")) {

      double val = dobj.doubleValue();
      if(exp.nbOperands() == 1) return -val;
      for(int i = 1; i < exp.nbOperands(); i++) {
        Object obj = evalExpValue(tuple, (ZExp)exp.getOperand(i));
        val -= ((Number)obj).doubleValue();
      }
      return val;

    } else if(op.equals("*")) {

      double val = dobj.doubleValue();
      for(int i = 1; i < exp.nbOperands(); i++) {
        Object obj = evalExpValue(tuple, (ZExp)exp.getOperand(i));
        val *= ((Number)obj).doubleValue();
      }
      return val;

    } else if(op.equals("/")) {

      double val = dobj.doubleValue();
      for(int i = 1; i < exp.nbOperands(); i++) {
        Object obj = evalExpValue(tuple, (ZExp)exp.getOperand(i));
        val /= ((Number)obj).doubleValue();
      }
      return val;

    } else if(op.equals("**")) {

      double val = dobj.doubleValue();
      for(int i = 1; i < exp.nbOperands(); i++) {
        Object obj = evalExpValue(tuple, (ZExp)exp.getOperand(i));
        val = Math.pow(val, ((Number)obj).doubleValue());
      }
      return val;

    } else {
      throw new SQLException("ZEval.evalNumericExp(): Unknown operator " + op);
    }
  }


  /**
   * Evaluate a numeric or string expression (example: a+1)
   * @param tuple The tuple on which to evaluate the expression
   * @param exp The expression to evaluate
   * @return The expression's value
   */
  public Object evalExpValue(ZTuple tuple, ZExp exp) throws SQLException {

    Object o2 = null;

    if(exp instanceof ZConstant) {

      ZConstant c = (ZConstant)exp;

      switch(c.getType()) {

        case ZConstant.COLUMNNAME:

          Object o1 = tuple.getAttValue(c.getValue());
          if(o1 == null)
            throw new SQLException("ZEval.evalExpValue(): unknown column "
             + c.getValue());
          try {
            o2 = new Double(o1.toString());
          } catch(NumberFormatException e) {
            o2 = o1;
          }
          break;

        case ZConstant.NUMBER:
          o2 = new Double(c.getValue());
          break;

        case ZConstant.STRING:
        default:
          o2 = c.getValue();
          break;
      }
    } else if(exp instanceof ZExpression) {
      o2 = new Double(evalNumericExp(tuple, (ZExpression)exp));
    }
    return o2;
  }


  // test
  public static void main(String args[]) {
    try {
      BufferedReader db = new BufferedReader(new FileReader("test.db"));
      String tpl = db.readLine();
      ZTuple t = new ZTuple(tpl);

      ZqlParser parser = new ZqlParser();
      ZEval evaluator = new ZEval();

      while((tpl = db.readLine()) != null) {
        t.setRow(tpl);
        BufferedReader sql = new BufferedReader(new FileReader("test.sql")); 
        String query;
        while((query = sql.readLine()) != null) {
          parser.initParser(new ByteArrayInputStream(query.getBytes()));
          ZExp exp = parser.readExpression();
          System.out.print(tpl + ", " + query + ", ");
          System.out.println(evaluator.eval(t, exp));
        }
        sql.close();
      }
      db.close();
    } catch(Exception e) {
      e.printStackTrace();
    }
  }
};

