package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZDelete;
import com.experlog.zql.ZExp;


/**
 * 
 * @author gkarvoun
 *
 */
public class SqlDelete extends ZDelete {
	String _alias;
	
	public SqlDelete(String tab) {
		super(tab);
	}
	
	public SqlDelete(String tab, String alias) {
		super(tab);
		_alias = alias;
	}
	
	public String toString(){
		ZExp w = getWhere();
		addWhere(null);
		
		StringBuffer buf = new StringBuffer(super.toString());
		if(_alias != null)
			buf.append(" " + _alias);
		
		addWhere(w);
		
		if(getWhere() != null) 
			buf.append(" where " + getWhere().toString());
		return buf.toString();
	}
};

