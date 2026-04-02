package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.experlog.zql.ZColumnDef;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;
import edu.upenn.cis.orchestra.exchange.flatfile.FileDb;
import edu.upenn.cis.orchestra.dbms.SqlDb;

/**
 * 
 * @author gkarvoun
 *
 */
public class SqlStatementGen {
	
	public static String sessionSchema = "SESSION";

	public String preparedParameterProjection(){
		return ("?");
	}

	public String nullProjection(String type){
		return ("null");
	}

	public String skolemNullProjection(String type){
//		return ("null");
//		HACK ...
		if(type.contains("INT") || type.contains("DOUBLE") ){
			return("0");
		}else{
			return("''");
		}
	}
	
	public String skolemColumnValue(String type){
		return("''");
	}

	public String caseNull(String name){
		return ("CASE WHEN " + name + " IS NOT NULL THEN 1 ELSE -1 END");
	}

	public List<String> importData(FileDb fdb, List<String> baseTables, SqlDb db){
		throw new RuntimeException("Unsupported database to import into");
	}
	
	public String runStats(Relation baseTable, AtomType type) {
		return null;
	}

	public String runStats(String tableName, AtomType type) {
		return null;
	}
	
//	
//	public String runStatistics(String tabName){
//		return null;
//	}

	public String getLoggingMsg() {
		return "";
	}
	
	public List<String> turnOffLoggingAndResetStats (String tabName)
	{
		return new ArrayList<String>();
	}
	
	public String clusterIndexSuffix(boolean noLogging)
	{
		return "";
	}

	public List<String> clearAndCopy(String oldTable, String newTable){
//		This may screw "volatile cardinality" - delete instead
		List<String> ret;
		SqlMove m;
		if (false /*Config.isDB2()*/) {
			//        	Alternative: ALTER TABLE … ACTIVATE NOT LOGGED INITIALLY WITH EMPTY TABLE	
			m = new SqlMove(oldTable, newTable, false);
			ret = m.toStringList();
			ret.addAll(turnOffLoggingAndResetStats(newTable));
		} else {
			m = new SqlMove(oldTable, newTable, true);
		}
		return m.toStringList();
//		ret.add(vol);
	}
	
	public String dropTable(String tabName)
	{
		SqlDrop d = new SqlDrop(tabName);
		return d.toString();
	}
	
	public String subtractTables(String pos, String neg, String joinAtt)
	{
		SqlDelete d = new SqlDelete(pos, "R1");
		SqlQuery q = new SqlQuery(new SqlSelectItem("1"),
				new SqlFromItem(neg + " R2"),
				new SqlExpression(SqlExpression.EQ, 
						new SqlConstant("R1." + joinAtt, SqlConstant.COLUMNNAME), 
						new SqlConstant("R2." + joinAtt, SqlConstant.COLUMNNAME)));
		
		SqlExpression expr = new SqlExpression(SqlExpression.EXISTS, q);
		d.addWhere(expr);
		
		return d.toString();
	}

	public String deleteTable(String tabName)
	{
		SqlDelete d = new SqlDelete(tabName);
		return d.toString();
	}

	public List<String> createTable(String tabName, Vector<ZColumnDef> cols, boolean noLogging){
		SqlCreate cr = new SqlCreate(tabName, "", cols, "");
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		return ret;
	}
	
	public List<String> createTempTable(String tabName, Vector<ZColumnDef> cols){
		SqlCreateTempTable cr = new SqlCreateTempTable(tabName, "", cols, this);
		
		List<String> ret = new ArrayList<String>();
		ret.add(cr.toString());
		return ret;
	}
	
	public String createIndex(String indName, String tabName, Vector<ZColumnDef> cols, boolean cluster, boolean noLogging){
		SqlCreateIndex cr;
		if(cluster && !indName.startsWith(sessionSchema))
			cr = new SqlCreateIndex(indName, tabName, cols, clusterIndexSuffix(noLogging));
		else
			cr = new SqlCreateIndex(indName, tabName, cols, "");
		return cr.toString();
	}
	
	public List<String> addColsToTable(String tabName, Vector<ZColumnDef> columns, boolean noLogging){
		List<String> ret = new ArrayList<String>();
		return ret;
	}

	public String addColMsg(String col, String type, String def){
//		Is this DB2 specific?
		return "ADD " + col + " " + type + " DEFAULT " + def;
	}
	
	public String copyTable(String oldName, String newName){return "";}
	
	public String compareTables(String table1, String table2){return "";}
	
	/**
	 * SQL expression to return up to 128 chars of an expression
	 * @param var
	 * @return
	 */
	public static String getFirst128Chars(String var) {
		if(Config.getDBMSversion() >= 9.5)
			return "SUBSTR(" + var + ",1,MIN(LENGTH(" + var + "), 128))";
		else
			return "SUBSTR(" + var + ",1,CASE WHEN LENGTH(" + var + ") > 128 THEN 128 ELSE LENGTH(" + var + ") END)";
	}
 
	public String getFirstRow() {
		return "LIMIT 1";
	}
	
}
