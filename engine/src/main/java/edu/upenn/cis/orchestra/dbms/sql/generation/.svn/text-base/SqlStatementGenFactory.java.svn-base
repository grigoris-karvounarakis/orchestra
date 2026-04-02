package edu.upenn.cis.orchestra.dbms.sql.generation;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.dbms.sql.vendors.DB2SqlStatementGen;
import edu.upenn.cis.orchestra.dbms.sql.vendors.HsqlSqlStatementGen;
import edu.upenn.cis.orchestra.dbms.sql.vendors.OracleSqlStatementGen;

public class SqlStatementGenFactory {
	public static SqlStatementGen createStatementGenerator() {
		if(Config.isDB2())
			return new DB2SqlStatementGen();
		else if(Config.isOracle())
			return new OracleSqlStatementGen();
		else if(Config.isHsql())
			return new HsqlSqlStatementGen();
		else
			return null;
	}
}
