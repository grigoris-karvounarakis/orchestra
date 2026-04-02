package edu.upenn.cis.orchestra.dbms.sql.vendors;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.experlog.zql.ZColumnDef;

import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlAlter;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlColumnDef;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlCreate;
import edu.upenn.cis.orchestra.dbms.sql.generation.SqlStatementGen;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;

/**
 * 
 * @author gkarvoun
 *
 */
public class OracleSqlStatementGen extends SqlStatementGen {

	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db){
		List<String> statements = new ArrayList<String>();
		try{
			statements.add(fdb.getSQLImportFromFile(AtomType.DEL));
			statements.add(fdb.getSQLImportFromFile(AtomType.INS));
		}catch(Exception e){
			e.printStackTrace();
		}
		return statements;
	}

	public String getLoggingMsg() {
		return " NOLOGGING";
	}
	
	public String clusterIndexSuffix(boolean noLogging)
	{
		String clustSpec = "";

		if (noLogging){
			clustSpec = clustSpec + getLoggingMsg();
		}
		return clustSpec;
	}
	
	public String alterColMsg(String col, String def){
		return "MODIFY(" + col + " DEFAULT " + def + ")";
	}
	
	public List<String> createTable(String tabName, Vector<ZColumnDef> columns, boolean noLogging){
		SqlCreate cr = new SqlCreate(tabName, "", columns, getLoggingMsg());
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		
		for (int i = 0; i < columns.size(); i++) {
			SqlColumnDef s = (SqlColumnDef)(columns.get(i));
			
			SqlAlter alt = new SqlAlter(tabName, SqlAlter.SET_DEF_VAL);
			
			if(s.getDefault() != null)
				ret.add(alt.toString() + alterColMsg(s.getName(), s.getDefault()));
		}

		return ret;
	}
	
	public List<String> addColsToTable(String tabName, Vector<ZColumnDef> columns, boolean noLogging){
		List<String> ret = new ArrayList<String>();
				 
		for (int i = 0; i < columns.size(); i++) {
			SqlColumnDef s = (SqlColumnDef)(columns.get(i));
			
			SqlAlter alt = new SqlAlter(tabName, SqlAlter.ADD_COLUMN);
			
			if(s.getDefault() != null)
				if(noLogging)
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()) + " " + getLoggingMsg());
				else
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()));
		}

		return ret;
	}

}
