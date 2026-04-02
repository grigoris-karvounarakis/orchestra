package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZExp;
import com.experlog.zql.ZExpression;


/**
 * SqlExpression: an SQL Expression
 * An SQL expression is an operator and one or more operands 
 * Example: a AND b AND c -> operator = AND, operands = (a, b, c)
 * 
 * @author gkarvoun
 */
public class SqlExpression extends ZExpression {

	public SqlExpression(int code) {
		super(code);
	}

	public SqlExpression(int code, ZExp o1) {
		super(code, o1);
	}

	public SqlExpression(int code, ZExp o1, ZExp o2) {
		super(code, o1, o2);
	}
	
	public static SqlExpression falseExp(){
		return new SqlExpression(SqlExpression.EQ, 
				new SqlConstant("1", SqlConstant.NUMBER), 
				new SqlConstant("2", SqlConstant.NUMBER));
	}
	
	public boolean isBoolean() {
		return (getCode() == ZExpression.AND ||
		getCode() == ZExpression.OR ||
		getCode() == ZExpression.NOT ||
		getCode() == ZExpression.ALL ||
		getCode() == ZExpression.LIKE ||
		getCode() == ZExpression.BETWEEN ||
		getCode() == ZExpression.EQ ||
		getCode() == ZExpression.NEQ ||
		getCode() == ZExpression.NEQ2 ||
		getCode() == ZExpression.LT ||
		getCode() == ZExpression.LTE ||
		getCode() == ZExpression.GT ||
		getCode() == ZExpression.GTE ||
		getCode() == ZExpression.NOT_BETWEEN ||
		getCode() == ZExpression.NOT_IN ||
		getCode() == ZExpression.NOT_IN);
	}
	
	public boolean equals(Object other)
	{
		if(!(other instanceof SqlExpression))
			return false;
		else
		{
			return getCode()== ((SqlExpression)other).getCode() && getOperands().equals(((SqlExpression)other).getOperands());
		}
	}
};

