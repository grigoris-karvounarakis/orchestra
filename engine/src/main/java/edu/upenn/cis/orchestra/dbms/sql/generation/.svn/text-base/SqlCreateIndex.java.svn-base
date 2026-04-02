package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.Vector;

import com.experlog.zql.ZColumnDef;

public class SqlCreateIndex implements SqlCreateStatement {
	protected String _indName;
	protected String _tabName;
	protected Vector<ZColumnDef> _columns;
	protected String _cluster;
	
	public SqlCreateIndex(String indName, String tabName, Vector<ZColumnDef> columns,
			String cluster) {
		_indName = indName;
		_tabName = tabName;
		_columns = columns;
		_cluster = cluster;
	}

	public String toString(){
		StringBuffer cols = new StringBuffer();
		for (int i = 0; i < _columns.size(); i++) {
			if (i > 0) {
				cols.append(", ");
			}
			cols.append(_columns.get(i).toString());
		}
//		String stmt = "CREATE " + _cluster + " INDEX " + _indName + " ON "
		String stmt = "CREATE INDEX " + _indName + " ON " 
			+ _tabName + " (" + cols + ") " + _cluster;

		return stmt;
	}
}
