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
public class HsqlSqlStatementGen extends SqlStatementGen {

	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db){
		List<String> statements = new ArrayList<String>();

		try{
			for (String s: baseTables) {
				if (s.endsWith("_L") || (false && s.endsWith("_R"))) {
					statements.add(fdb.getSQLImportCommandHsqlDb(s, 
							db.getAttributes(s), db.getTypes(s), AtomType.DEL));

					//statements.add(updateStatCommand(s + "_DEL"));

					statements.add(fdb.getSQLImportCommandHsqlDb(s, 
							db.getAttributes(s), db.getTypes(s), AtomType.INS));

					//statements.add(updateStatCommand(s + "_INS"));
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
		return statements;
	}
	
	public List<String> createTable(String tabName, Vector<ZColumnDef> cols, boolean noLogging){
		SqlCreate cr = new SqlCreate(tabName, "CACHED", cols, getLoggingMsg());
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
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
