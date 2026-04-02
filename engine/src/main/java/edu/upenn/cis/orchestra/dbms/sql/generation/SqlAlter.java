package edu.upenn.cis.orchestra.dbms.sql.generation;


public class SqlAlter implements SqlStatement {
	public String _table;
	public int _type;
	
	public static int SET_DEF_VAL = 1;
	public static int DROP_CONSTRAINT = 2;
	public static int NO_LOGGING = 3;
	public static int ADD_COLUMN = 3;
	
	public SqlAlter(String table, int type) {
		_table = table;
		_type = type;
	}
	

	public String toString() {
//		if(_type == SET_DEF_VAL)
			return "ALTER TABLE " + _table + " ";
	}
}
