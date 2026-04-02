package edu.upenn.cis.orchestra.dbms.sql.generation;

public class SqlRename implements SqlStatement {

	String _table1;
	String _table2;
	
	public SqlRename() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Create a RENAME statement on a given table
	 * @param tab the table name
	 */
	public SqlRename(String tab1, String tab2) {
		_table1 = new String(tab1);
		_table2 = new String(tab2);
	}

	/**
	 * @return The name of the table before the RENAME statement.
	 */
	public String getOldTable() { return _table1; }

	/**
	 * @return The name of the table after the RENAME statement.
	 */
	public String getNewTable() { return _table2; }
	
	public String toString() {
		StringBuffer buf = new StringBuffer("rename table ");
		buf.append(_table1);
		buf.append(" to ");
		buf.append(_table2);
		return buf.toString();
	}
}
