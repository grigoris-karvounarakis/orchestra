package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZAliasedName;

/**
 * 
 * @author gkarvoun
 *
 */
public class SqlAliasedName extends ZAliasedName{

	public SqlAliasedName() {
		super();
	}

	public SqlAliasedName(String fullname, int form) {
		super(fullname, form);
	}

}

