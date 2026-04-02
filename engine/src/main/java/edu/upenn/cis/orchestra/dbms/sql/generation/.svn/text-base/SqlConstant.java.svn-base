package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZConstant;


/**
 * 
 * @author gkarvoun
 *
 */
public class SqlConstant extends ZConstant {

	public SqlConstant(String v, int typ) {
		super(v, typ);
	}
	public boolean equals(Object other)
	  {
		  if(other == this)
			  return true;
		  else if(!(other instanceof SqlConstant))
			  return false;
		  else
			 return ((SqlConstant)other).getType() == getType() &&
			 ((SqlConstant)other).getValue().equals(getValue());
	  }
};

