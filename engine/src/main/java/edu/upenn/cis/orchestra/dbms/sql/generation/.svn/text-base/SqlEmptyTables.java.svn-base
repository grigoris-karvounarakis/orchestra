package edu.upenn.cis.orchestra.dbms.sql.generation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.List;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.dbms.SqlDb;

public class SqlEmptyTables {
	List<String> _tables;

	SqlEmptyTables(List<String> tables) {
		_tables = tables;
	}

	public boolean emptyTables(SqlDb db) throws SQLException {
		Calendar before = Calendar.getInstance();
		if(Config.getRunStatistics()){
			for (String t : _tables) {
				ResultSet res = db.evaluateQuery("SELECT 1 FROM " + t + " " + db.getSqlTranslator().getFirstRow());

				if (!res.next()){
					Calendar after = Calendar.getInstance();
					long time = after.getTimeInMillis() - before.getTimeInMillis();
					Debug.println("EMPTY TABLE CHECK TIME: " + time + " msec");
					db.time4EmptyChecking += time;
					res.close();
					return true;
				}
				res.close();
			}
		}
		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();
		Debug.println("EMPTY TABLE CHECK TIME: " + time + " msec");
		db.time4EmptyChecking += time;
		return false;
	}
}
