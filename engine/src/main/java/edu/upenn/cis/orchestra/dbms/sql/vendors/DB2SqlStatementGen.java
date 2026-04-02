package edu.upenn.cis.orchestra.dbms.sql.vendors;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import com.experlog.zql.ZColumnDef;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationField;
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
public class DB2SqlStatementGen extends SqlStatementGen {

	public String preparedParameterProjection(){
		return ("CAST(? AS INTEGER)");
	}

	public String nullProjection(String type){
		return ("cast(null as " + type + ")");
	}

	public String skolemNullProjection(String type){
//		return ("cast(null as " + type + ")");
//		HACK ...
		if(type.contains("INT") || type.contains("DOUBLE") ){
			return("cast(0 as " + type + ")");
		}else{
			return("cast('empty' as " + type + ")");
		}
	}
	
	public String skolemColumnValue(String type){
//		return ("cast(null as " + type + ")");
//		HACK ...
		if(type.contains("INT") || type.contains("DOUBLE") ){
			return("0");
		}else{
			return("'empty'");
		}
	}

	
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

	@Override
	public String runStats(Relation baseTable, AtomType type) {
		return runStats(baseTable.getFullQualifiedDbId(), type);
	}

	public String rebindAllPlans(){
		return "SELECT SUBSTR('REBIND PLAN('CONCAT NAME " +
		"  CONCAT')                        ',1,45) " +
		"FROM SYSIBM.SYSPLAN;" ;
	}

	@Override
	public String runStats(String tableName, AtomType type) {
		if(!Config.getRunStatistics()){
			if (!Config.getTempTables() || !tableName.contains(SqlStatementGen.sessionSchema)){
				return "ALTER TABLE " + tableName + " VOLATILE CARDINALITY";
			}else{
				return null;
			}
		}else{
			String suffix = "";
			if(!AtomType.NONE.equals(type))
				suffix += "_" + type.toString();
			return  "call ADMIN_CMD('RUNSTATS ON TABLE " 
			+ tableName + suffix + " ON KEY COLUMNS ALLOW WRITE ACCESS')";
//			+ tableName + suffix + " ON ALL COLUMNS ALLOW WRITE ACCESS')";
//			+ tableName + suffix + " ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
//			+ tableName + suffix + " WITH DISTRIBUTION ON ALL COLUMNS ALLOW WRITE ACCESS')";
//			+ tableName + suffix + " WITH DISTRIBUTION ON ALL COLUMNS AND DETAILED INDEXES ALL ALLOW WRITE ACCESS')";
//			return null;
		}
	}

	
//	@Override
//	public String runStatistics(String tabName){
//		return "ADMIN_CMD(RUNSTATS ON TABLE " + tabName + 
//		" ON ALL COLUMNS AND INDEXES ALL ALLOW WRITE ACCESS SET PROFILE)";
//	}
	
	public String getLoggingMsg() {
		if (Config.getTempTables())
			return "";
		else
			return " NOT LOGGED INITIALLY";
	}

	public String getLoggingMsg4Alter() {
		return " NOT LOGGED INITIALLY";
	}

	public List<String> turnOffLoggingAndResetStats (String tabName)
	{
		List<String> statements = new ArrayList<String>();

		if (!Config.getTempTables() || !tabName.contains(SqlStatementGen.sessionSchema)) {
			Debug.println("ALTER TABLE " + tabName + " ACTIVATE" + getLoggingMsg4Alter());
			statements.add("ALTER TABLE " + tabName + " ACTIVATE" + getLoggingMsg4Alter());
		}
		return statements;
	}

		
	public String clusterIndexSuffix(boolean noLogging)
	{
		String clustSpec = "";
		clustSpec = "CLUSTER";

//		if (noLogging){
//			clustSpec = clustSpec + getLoggingMsg();
//		}
		return clustSpec;
	}

	public String alterColMsg(String col, String def){
		return "ALTER COLUMN " + col + " SET DEFAULT " + def;
	}

	public String addColMsg(String col, String type, String def){
		return "ADD " + col + " " + type + " DEFAULT " + def;
	}
	
	public List<String> createTable(String tabName, Vector<ZColumnDef> columns, boolean noLogging){
		SqlCreate cr = new SqlCreate(tabName, "", columns, getLoggingMsg());
				
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		
		for (int i = 0; i < columns.size(); i++) {
			SqlColumnDef s = (SqlColumnDef)(columns.get(i));
			
			SqlAlter alt = new SqlAlter(tabName, SqlAlter.SET_DEF_VAL);
			
			if(s.getDefault() != null)
				if(noLogging)
					ret.add(alt.toString() + alterColMsg(s.getName(), s.getDefault()) + " ACTIVATE" + getLoggingMsg());
				else
					ret.add(alt.toString() + alterColMsg(s.getName(), s.getDefault()));		}

		return ret;
	}
	
	public List<String> addColsToTable(String tabName, Vector<ZColumnDef> columns, boolean noLogging){
		List<String> ret = new ArrayList<String>();
				
		for (int i = 0; i < columns.size(); i++) {
			SqlColumnDef s = (SqlColumnDef)(columns.get(i));
			
			SqlAlter alt = new SqlAlter(tabName, SqlAlter.ADD_COLUMN);
			
			if(s.getDefault() != null)
				if(noLogging)
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()) + " ACTIVATE" + getLoggingMsg());
				else
					ret.add(alt.toString() + addColMsg(s.getName(), s.getType(), s.getDefault()));
		}

		return ret;
	}
	
	public String copyTable(String oldName, String newName){
		return new String(
			"EXPORT TO \"WEBCPTAB.IXF\" OF IXF MESSAGES \"WEBCPTAB.EXM\" SELECT * FROM " + oldName + ";\n" + 
			"IMPORT FROM \"WEBCPTAB.IXF\" OF IXF MESSAGES \"WEBCPTAB.IMM\" CREATE INTO " + newName + ";\n"
//			"EXPORT TO \"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.IXF\" OF IXF MESSAGES\n" +
//			"\"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.EXM\" SELECT * FROM " + oldName + ";\n" + 
//			"IMPORT FROM \"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.IXF\" OF IXF MESSAGES\n" +
//			"\"C:\\PROGRA~1\\IBM\\SQLLIB\\WEBCPTAB.IMM\" CREATE INTO " + newName + ";\n"
		);
	}
	
	public String explainQuery(String queryString, int n){
		return new String("ADMIN_CMD(explain all set queryno = " + n + " for " + queryString + ")");
	}

	public String getExplainedQueryEstimatedCostAndCardinality(int n){
		return new String(
				"select S.TOTAL_COST, C.STREAM_COUNT\n" + 
				"from EXPLAIN_STATEMENT S, EXPLAIN_STREAM C\n" +
				"where S.EXPLAIN_TIME = C.EXPLAIN_TIME and C.TARGET_ID = 1\n" +
				"and S.EXPLAIN_LEVEL = 'P' and S.QUERYNO = " + n);
	}

	public String compareTables(String table1, String table2)
	{
		return new String(
				"select COUNT(*) " +
				"from ((select * from " + table1 + " except select * from " + table2 + ") union " +
				"      (select * from " + table2 + " except select * from " + table1 + ")) AS ZZZ"
		);
	}


	/**
	 * Generate SQL to update the system catalog relations
	 * 
	 */
	public List<String> updateCatalogInfo(Relation s, int updatedCardinality, int pages,
			Map<RelationField,Integer> columnCardinalities, Map<RelationField,Integer> avgLengths) {
		
		List<String> ret = new ArrayList<String>();
		
		ret.add("UPDATE SYSSTAT.COLUMNS SET COLCARD=-1, NUMNULLS=-1 WHERE TABNAME = '" +
				s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");

		ret.add("UPDATE SYSSTAT.TABLES SET CARD=" + updatedCardinality + ", NPAGES=" + pages + 
				", FPAGES=" + pages + ", OVERFLOW=0, ACTIVE_BLOCKS=0 WHERE TABNAME = '" +
				s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");
		
		if (columnCardinalities != null)
			for (RelationField a : columnCardinalities.keySet()) {
				ret.add("UPDATE SYSSTAT.COLUMNS SET COLCARD=" + columnCardinalities.get(a) + ", NUMNULLS=0 WHERE TABNAME = '" +
						s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");
			}
		if (avgLengths != null)
			for (RelationField a : avgLengths.keySet()) {
				ret.add("UPDATE SYSSTAT.COLUMNS SET AVGCOLLEN=" + avgLengths.get(a) + " WHERE TABNAME = '" +
						s.getDbRelName() + "' AND TABSCHEMA = '" + s.getDbSchema() + "'");
			}
		return ret;
	}
	
	public String getFirstRow() {
		return "FETCH FIRST 1 ROWS ONLY";
	}
}