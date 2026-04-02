package edu.upenn.cis.orchestra.dbms.sql.generation;

public class SqlDrop implements SqlStatement {

	String _table;
	  
	public SqlDrop() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Create a DROP statement on a given table
	 * @param tab the table name
	 */
	public SqlDrop(String tab) {
		_table = new String(tab);
	}

	/**
	 * @return The table concerned by the DROP statement.
	 */
	public String getTable() { return _table; }

	public String toString() {
		StringBuffer buf = new StringBuffer("DROP TABLE ");
		buf.append(_table);
		return buf.toString();
	}
}
