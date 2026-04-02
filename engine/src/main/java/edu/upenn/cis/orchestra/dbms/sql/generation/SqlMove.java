package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.ArrayList;
import java.util.List;

public class SqlMove implements SqlStatement {
	boolean _soft; // true means delete/copy/delete, false means drop/rename/create

	protected String _schema1;
	protected String _schema2;
	protected String _table1;
	protected String _table2;
	
	public SqlMove() {
		// TODO Auto-generated constructor stub
	}
	
	/**
	 * Create a RENAME statement on a given table
	 * @param tab the table name
	 */
	public SqlMove(String tab1, String tab2, boolean soft) {
		int dot1 = tab1.lastIndexOf(".");
		int dot2 = tab2.lastIndexOf(".");
		
		if(dot1 == -1){
			_schema1 = null;
			_table1 = new String(tab1);
		}else{
			_schema1 = new String(tab1.substring(0, dot1));
			_table1 = new String(tab1.substring(dot1+1, tab1.length()));
		}
		
		if(dot2 == -1){
			_schema2 = null;
			_table2 = new String(tab2);
		}else{
			_schema2 = new String(tab2.substring(0, dot2));
			_table2 = new String(tab2.substring(dot2+1, tab2.length()));
		}		
		_soft = soft;
	}

	/**
	 * @return The name of the table before the RENAME statement.
	 */
	public String getOldTable() { 
		if(_schema1 != null)
			return _schema1 + "." + _table1; 
		else
			return _table1;
	}

	public String getOldTableWithoutSchema() { return _table1; }
	
	/**
	 * @return The name of the table after the RENAME statement.
	 */
	public String getNewTable() { 
		if(_schema2 != null)
			return _schema2 + "." + _table2; 
		else
			return _table2;
	}
	
	public String getNewTableWithoutSchema() { return _table2; }
	
	public List<String> toStringList() {
		List<String> ret = new ArrayList<String>();
		
		if(_soft){
			ret.add("DELETE FROM " + getOldTable());
			ret.add("INSERT INTO " + getOldTable() + " SELECT * FROM " + getNewTable());
			ret.add("DELETE FROM " + getNewTable());
		}else{
			SqlDrop dr = new SqlDrop(getOldTable());
			SqlRename ren = new SqlRename(getNewTable(), getOldTableWithoutSchema());
			
			SqlSelectItem star = new SqlSelectItem("*");
			SqlFromItem fr = new SqlFromItem(getOldTable());
			
			SqlExpression w = SqlExpression.falseExp();
			SqlQuery cq = new SqlQuery(star, fr, w);
			SqlCreate cr = new SqlCreate(getNewTable(), cq);
			
//			String drop = new String("DROP TABLE " + getOldTable());
//			//  New RENAME name should not include schema name ...
//			//        	String rename = new String("RENAME TABLE " + newA.toString3() + " TO " + oldA.toString3());
//			String rename = new String("RENAME TABLE " + getNewTableWithoutSchema() + " TO " + getOldTable());
//			String create = new String("CREATE TABLE " + getNewTable() + 
//					" AS (SELECT * FROM " + getOldTable() + " WHERE 1=2) DEFINITION ONLY");      	

			//String create = new String("CREATE TABLE " + newA.toString3() + " AS (SELECT * FROM " + oldA.toString3() + " WHERE 1=2) WITH NO DATA");

			ret.add(dr.toString());
			ret.add(ren.toString());
			ret.add(cr.toString());
		}
		return ret;
	}
	
	public String toString() {
		StringBuffer buf = new StringBuffer("");
		return buf.toString();
	}
}
