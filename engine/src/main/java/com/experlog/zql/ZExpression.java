package com.experlog.zql;

import java.util.Vector;

/**
 * ZExpression: an SQL Expression
 * An SQL expression is an operator and one or more operands 
 * Example: a AND b AND c -> operator = AND, operands = (a, b, c)
 */
public class ZExpression implements ZExp {

  Vector<ZExp> operands_ = null;
  String op_;
  protected int code_;

  public static final int AND = 0;
  public static final int OR = 1;
  public static final int IN = 2;
  public static final int NOT_IN = 3;
  public static final int BETWEEN = 4;
  public static final int NOT_BETWEEN = 5;
  public static final int LIKE = 6;
  public static final int NOT_LIKE = 7;
  public static final int IS_NULL = 8;
  public static final int IS_NOT_NULL = 9;
  public static final int STARSSIGN = 10;
  public static final int COMMA = 11;
  public static final int UNION = 12;
  public static final int UNION_ALL = 13;
  public static final int INTERSECT = 14;
  public static final int MINUS = 15;
  public static final int NOT = 16;
  public static final int EXISTS = 17;
  public static final int EXCEPT = 18;
  public static final int PRIOR = 19;
  public static final int ALL = 20;
  public static final int ANY = 21;
  public static final int QUESTIONMARK = 22;
  public static final int PLUSSIGN = 23;
  public static final int MINUSSIGN = 24;
  public static final int MULTSIGN = 25;
  public static final int DIVSIGN = 26;
  public static final int PIPESSIGN = 27;
  public static final int EQ = 28;
  public static final int NEQ = 29;
  public static final int LT = 30;
  public static final int LTE = 31;
  public static final int GT = 32;
  public static final int GTE = 33;
  public static final int COUNT = 34;
  public static final int SUM = 35;
  public static final int MAX = 36;
  public static final int AVG = 37;
  public static final int MIN = 38;
  public static final int NEQ2 = 39;
  public static final int POUND = 40;
  public static final int EMPTY = 41;
  public static final int _NOT_SUPPORTED = 100;

  /**
   * Create an SQL Expression given the operator
   * @param op The operator
   */
  public ZExpression(int code) {
    code_ = code;
    op_ = opToString(code);
  }

  /**
   * Create an SQL Expression given the operator and 1st operand
   * @param op The operator
   * @param o1 The 1st operand
   */
  public ZExpression(int code, ZExp o1) {
    code_ = code;
    op_ = opToString(code);
    addOperand(o1);
  }

  /**
   * Create an SQL Expression given the operator, 1st and 2nd operands
   * @param op The operator
   * @param o1 The 1st operand
   * @param o2 The 2nd operand
   */
  public ZExpression(int code, ZExp o1, ZExp o2) {
    code_ = code;
    op_ = opToString(code);
    addOperand(o1);
    addOperand(o2);
  }
  
  public void setOperator(String operator)
  {
	  if(!(code_ == _NOT_SUPPORTED))
		  throw new RuntimeException("Can only set operator if code is not supported.");
	  op_ = operator;
  }

  /**
   * Get this expression's operator as a String.
   * @return the operator.
   */
  public String getOperator() { return op_; }
  
  /**
   * Get this expression's operator.
   * @return the operator.
   */
  public int getCode() { return code_; }

  /**
   * Set the operands list
   * @param v A vector that contains all operands (ZExp objects).
   */
  public void setOperands(Vector<ZExp> v) {
    operands_ = v;
  }

  /**
   * Get this expression's operands.
   * @return the operands (as a Vector of ZExp objects).
   */
  public Vector<ZExp> getOperands() {
    return operands_;
  }

  /**
   * Add an operand to the current expression.
   * @param o The operand to add.
   */
  public void addOperand(ZExp o) {
    if(operands_ == null) operands_ = new Vector<ZExp>();
    operands_.addElement(o);
  }

  /**
   * Get an operand according to its index (position).
   * @param pos The operand index, starting at 0.
   * @return The operand at the specified index, null if out of bounds.
   */
  public ZExp getOperand(int pos) {
    if(operands_ == null || pos >= operands_.size()) return null;
    return (ZExp)operands_.elementAt(pos);
  }

  /**
   * Get the number of operands
   * @return The number of operands
   */
  public int nbOperands() {
    if(operands_ == null) return 0;
    return operands_.size();
  }

  /**
   * String form of the current expression (reverse polish notation).
   * Example: a > 1 AND b = 2 -> (AND (> a 1) (= b 2))
   * @return The current expression in reverse polish notation (a String)
   */
  public String toReversePolish() {
    StringBuffer buf = new StringBuffer("(");
    buf.append(op_);
    for(int i = 0; i < nbOperands(); i++) {
      ZExp opr = getOperand(i);
      if(opr instanceof ZExpression)
        buf.append(" " + ((ZExpression)opr).toReversePolish());
      else if(opr instanceof ZQuery)
        buf.append(" (" + opr.toString() + ")");
      else
        buf.append(" " + opr.toString());
    }
    buf.append(")");
    return buf.toString();
  }

  public String toString() {

      if (code_ == EMPTY) return "";
      
    if(code_ == QUESTIONMARK) return op_; // For prepared columns ("?")

    if(ZUtils.isCustomFunction(op_) > 0)
      return formatFunction();

    StringBuffer buf = new StringBuffer();
    if(needPar(code_)) buf.append("(");

    ZExp operand;
    switch(nbOperands()) {

      case 1:
    	  operand = getOperand(0);
    	  if(operand instanceof ZConstant) {
    		
//   Greg: added this to deal with comma with one element
    		  if(code_ == COMMA)
    			  buf.append(operand.toString());
// Operator may be an aggregate function (MAX, SUM...)
    		  else if(ZUtils.isAggregate(op_))
    				  buf.append(op_ + "(" + operand.toString() + ")");
    			  else if(code_ == IS_NULL || code_ == IS_NOT_NULL)
    				  buf.append(operand.toString() + " " + op_);
    			  else buf.append(op_ + " " + operand.toString());
    	  } else if(operand instanceof ZQuery) {
    		  buf.append(op_ + " (" + operand.toString() + ")");
    	  } else {
    		  if(code_ == IS_NULL || code_ == IS_NOT_NULL)
    			  buf.append(operand.toString() + " " + op_);
    		  else buf.append(op_ + " " + operand.toString());
    	  }
    	  break;

      case 3:
        if(code_ == BETWEEN || code_ == NOT_BETWEEN) {
          buf.append(getOperand(0).toString() + " " + op_ + " "
           + getOperand(1).toString()
           + " AND " + getOperand(2).toString()); 
          break;
        }

      default:

        boolean in_op = (code_ == IN || code_ == NOT_IN);

        int nb = nbOperands();
        for(int i = 0; i < nb; i++) {

          if(in_op && i==1) buf.append(" " + op_ + " (");

          operand = getOperand(i);
          if(operand instanceof ZQuery && !in_op) {
            buf.append("(" + operand.toString() + ")");
          } else {
            buf.append(operand.toString());
          }
          if(i < nb-1) {
            if(code_ == COMMA || (in_op && i>0)) buf.append(", ");
            else if(!in_op) buf.append(" " + op_ + " ");
          }
        }
        if(in_op) buf.append(")");
        break;
    }
    if(needPar(code_)) buf.append(")");
    return buf.toString();
  }

  private boolean needPar(int code) {
    return ! (code == ANY || code == ALL
     || code == UNION || code == UNION_ALL || code == EXCEPT || ZUtils.isAggregate(code));
  }

  private String formatFunction() {
    StringBuffer b = new StringBuffer(op_ + "(");
    int nb = nbOperands();
    for(int i = 0; i < nb; i++) {
      b.append(getOperand(i).toString() + (i < nb-1 ? "," : ""));
    }
    b.append(")");
    return b.toString();
  }
  
  private static String opToString(int code) {
      switch (code) {
      case AND: return "and";
      case OR: return "or";
      case IN: return "in";
      case NOT_IN: return "not in";
      case BETWEEN: return "between";
      case NOT_BETWEEN: return "not between";
      case LIKE: return "like";
      case NOT_LIKE: return "not like";
      case IS_NULL: return "is null";
      case IS_NOT_NULL: return "is not null";
      case COUNT: return "count";
      case COMMA: return "comma";
      case UNION: return "union";
      case UNION_ALL: return "union all";
      case INTERSECT: return "intersect";
      case MINUS: return "minus";
      case NOT: return "not";
      case EXCEPT: return "except";
      case EXISTS: return "exists";
      case PRIOR: return "prior";
      case ALL: return "all";
      case ANY: return "any";
      case QUESTIONMARK: return "?";
      case PLUSSIGN: return "+";
      case MINUSSIGN: return "-";
      case MULTSIGN: return "*";
      case DIVSIGN: return "/";
      case PIPESSIGN: return "||";
      case STARSSIGN: return "**";
      case SUM: return "sum";
      case MAX: return "max";
      case AVG: return "avg";
      case MIN: return "min";
      case EQ: return "=";
      case NEQ: return "<>";
      case LT: return "<";
      case LTE: return "<=";
      case GT: return ">";
      case GTE: return ">=";
      case NEQ2: return "!=";
      case POUND: return "#";
      case _NOT_SUPPORTED: return "_NOT_SUPPORTED";
      case EMPTY: return "";
      default: 
          throw new RuntimeException("missing string for code " + code);
      }
  }
};

