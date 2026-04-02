package edu.upenn.cis.orchestra.dbms.sql.generation;

import com.experlog.zql.ZExp;
import com.experlog.zql.ZOrderBy;

/**
 * An SQL query ORDER BY clause.
 * 
 * @author gkarvoun
 * 
 */
public class SqlOrderBy extends ZOrderBy{

	public SqlOrderBy(ZExp e) { super(e); }

};

