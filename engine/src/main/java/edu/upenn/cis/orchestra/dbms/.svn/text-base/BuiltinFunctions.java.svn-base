package edu.upenn.cis.orchestra.dbms;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.experlog.zql.ZExp;

import edu.upenn.cis.orchestra.datamodel.*;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlConstant;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlExpression;

/**
 * SQL expressions corresponding to "built in functions"
 * 
 * @author zives
 *
 */
public class BuiltinFunctions {
	protected static Map<String,Schema> _builtIns;

	public static void setBuiltins(Map<String,Schema> builtIns) {
		_builtIns = builtIns;
	}

	/**
	 * Is this schema / function combo something built in?
	 * 
	 * @param sch
	 * @param fn
	 * @return
	 */
	public static boolean isBuiltIn(String sch, String fn) {
		if (!_builtIns.keySet().contains(sch))
			return false;

		if (_builtIns.get(sch).getIDForName(fn) == -1)
			return true;
		
		return true;
	}
	
	public static boolean isBuiltIn(String sch, String fn, Map<String,Schema> builtIns) {
		if (!builtIns.keySet().contains(sch))
			return false;

		if (builtIns.get(sch).getIDForName(fn) == -1)
			return true;
		
		return true;
	}
	

	public static SqlExpression getExpression(String sch, String fn, List<ZExp> args) {
		if (sch.equals("COMPARE")) {
			if (fn.equals("INTLESS")) {
				return new SqlExpression(SqlExpression.LT, args.get(0), args.get(1));
			} else if (fn.equals("INTLESSEQUAL")) {
				return new SqlExpression(SqlExpression.LTE, args.get(0), args.get(1));
			} else if (fn.equals("INTGREATER")) {
				return new SqlExpression(SqlExpression.GT, args.get(0), args.get(1));
			} else if (fn.equals("INTGREATEREQUAL")) {
				return new SqlExpression(SqlExpression.GTE, args.get(0), args.get(1));
			} else if (fn.equals("INTEQUAL")) {
				return new SqlExpression(SqlExpression.EQ, args.get(0), args.get(1));
			} else if (fn.equals("INTNOTEQUAL")) {
				return new SqlExpression(SqlExpression.NEQ, args.get(0), args.get(1));
			} else if (fn.equals("STRLIKE")) {
				return new SqlExpression(SqlExpression.LIKE, args.get(0), args.get(1));
			}  
		} else if (sch.equals("ARITH")) {
			if (fn.equals("INTADD")) {
				return new SqlExpression(SqlExpression.PLUSSIGN, args.get(0), args.get(1));
			} else if (fn.equals("INTSUB")) {
				return new SqlExpression(SqlExpression.MINUSSIGN, args.get(0), args.get(1));
			} else if (fn.equals("INTMUL")) {
				return new SqlExpression(SqlExpression.MULTSIGN, args.get(0), args.get(1));
			} else if (fn.equals("INTDIV")) {
				return new SqlExpression(SqlExpression.DIVSIGN, args.get(0), args.get(1));
			}  
		} else if (sch.equals("STRING")) {
			if (fn.equals("STRCAT")) {
				return new SqlExpression(SqlExpression.PIPESSIGN, args.get(0), args.get(1));
				/*
			} else if (fn.equals("SUBSTR")) {
				return new SqlExpression(SqlExpression.)
			} else if (fn.equals("STRLEN")) {
				return "LENGTH(" + args.get(1) + ")";
				*/
			}  
		}
		else if (sch.equals("EQUALITYUDFSL")|| sch.equals("EQUALITYUDFSR")){
			SqlExpression s = new SqlExpression(SqlExpression._NOT_SUPPORTED, args.get(0));
			s.setOperator(fn);
			for(int i=1;i<args.size();i++)
				s.addOperand(args.get(i));
			return s;
			
		}
		return null;
	}
	
	public static String getResultString(String sch, String fn, List<String> args) {
		if (sch.equals("COMPARE")) {
			if (fn.equals("INTLESS")) {
				return "(" + args.get(0) + " < " + args.get(1) + ")";
			} else if (fn.equals("INTLESSEQUAL")) {
				return "(" + args.get(0) + " <= " + args.get(1) + ")";
			} else if (fn.equals("INTGREATER")) {
				return "(" + args.get(0) + " > " + args.get(1) + ")";
			} else if (fn.equals("INTGREATEREQUAL")) {
				return "(" + args.get(0) + " >= " + args.get(1) + ")";
			} else if (fn.equals("INTEQUAL")) {
				return "(" + args.get(0) + " = " + args.get(1) + ")";
			} else if (fn.equals("INTNOTEQUAL")) {
				return "(" + args.get(0) + " <> " + args.get(1) + ")";
			} else if (fn.equals("STRLIKE")) {
				return args.get(0) + " LIKE '" + args.get(1) + "'";
			}  
		} else if (sch.equals("ARITH")) {
			if (fn.equals("INTADD")) {
				return "(" + args.get(0) + " + " + args.get(1) + ")";
			} else if (fn.equals("INTSUB")) {
				return "(" + args.get(0) + " - " + args.get(1) + ")";
			} else if (fn.equals("INTMUL")) {
				return "(" + args.get(0) + " * " + args.get(1) + ")";
			} else if (fn.equals("INTDIV")) {
				return "(" + args.get(0) + " / " + args.get(1) + ")";
			}  
		} else if (sch.equals("STRING")) {
			if (fn.equals("STRCAT")) {
				return "(" + args.get(0) + " || " + args.get(1) + ")";
			} else if (fn.equals("SUBSTR")) {
				return "SUBSTR(" + args.get(0) + "," + args.get(1) + "," + args.get(2) + ")";
			} else if (fn.equals("STRLEN")) {
				return "LENGTH(" + args.get(0) + ")";
			}  
		} else if(sch.equals("EQUALITYUDFSL")|| sch.equals("EQUALITYUDFSR")){
			StringBuffer arglist= new StringBuffer();
			arglist.append("(");
			if(args.size()>0)
				arglist.append(args.get(0));
			for(int i=1;i<args.size();i++)
			{
				arglist.append(",");
				arglist.append(args.get(i));
			}
			arglist.append(")");
			return fn +arglist.toString();
		}
		return "";
	}

	public static String evaluateBuiltIn(Atom atom, Map<String,String> varmap,
			Map<String,SqlExpression> whereExpressions, Set<SqlExpression> whereRoots, List<Atom> allAtoms) {
		String sch = atom.getSchema().getSchemaId();
		String fn = atom.getRelation().getName();
		
		List<String> args = new ArrayList<String>();
		List<ZExp> children = new ArrayList<ZExp>();
		//args.add(atom.getValues().get(0).toString());
		
		// Make sure the parameters are defined
		
		for (int i = 0/*getFirstParm(sch, fn)*/; i < atom.getValues().size(); i++) {
			String arg = atom.getValues().get(i).toString();
			if (atom.getValues().get(i) instanceof AtomConst) {
				args.add(arg);
				try {
					Integer.valueOf(arg);
					children.add(new SqlConstant(arg, SqlConstant.NUMBER));
				} catch (NumberFormatException ne) {
					
					// Trim '' because the parent class already adds these to a string
					if (arg.charAt(0) == '\'' && arg.charAt(arg.length() - 1) == '\'') {
						arg = arg.substring(1, arg.length() - 1);
					}
					children.add(new SqlConstant(arg, SqlConstant.STRING));
				}
			} else if (varmap.get(arg) == null && i >= getFirstParm(sch, fn))
				return "";
			else if (i >= getFirstParm(sch, fn)) {
				args.add(getArgumentForVar(fn,arg,i,varmap,allAtoms));
				if (whereExpressions.get(arg) != null)
					children.add(whereExpressions.get(arg));
				else
					children.add(new SqlConstant(getArgumentForVar(fn,arg,i,varmap,allAtoms), SqlConstant.COLUMNNAME));
				// This is no longer the root of a condition -- it's a subexpression
//				_whereRoots.remove(_whereExpressions.get(arg));
			}
		}
		
		SqlExpression newExpr = BuiltinFunctions.getExpression(sch, fn, children);
		
		if (newExpr.isBoolean())
			whereRoots.add(newExpr);
		else
			whereExpressions.put(atom.getValues().get(0).toString(), newExpr);
		
		return BuiltinFunctions.getResultString(sch, fn, args);
	}
	
	protected static String getArgumentForVar(String fn,String arg, int pos,Map<String, String> varmap, List<Atom> allAtoms){
		if(!UDFunctions.isUDF(fn))
			return varmap.get(arg);
		else{
			Relation rel = UDFunctions.argumentFor(fn, new Integer(pos), null);
			RelationField rf = UDFunctions.argumentFor(fn, new Integer(pos));
			int atomIndex=0;
			for(Atom a:allAtoms){
				if(a.getRelation().equals(rel))
					break;
				atomIndex++;
			}
			return RuleSqlGen.fullNameForAttr(rf, atomIndex);
		}
	}
	protected static int getFirstParm(String sch, String fn) {
		if (sch.equals("COMPARE"))
			return 0;
		else
			return 1;
	}
}
