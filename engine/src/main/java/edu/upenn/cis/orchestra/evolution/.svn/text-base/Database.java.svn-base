package edu.upenn.cis.orchestra.evolution;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.upenn.cis.orchestra.Config;

public class Database {

	protected String m_url;
	protected Properties m_props;
	protected Connection m_connection;
	protected Statement m_statement;

	protected int m_optimizerCalls;
	protected int m_cacheHits;
	protected long m_optimizerTime;
//	protected static Logger m_logger = Logger.getLogger(Database.class.getName());
	protected static Logger m_logger = Logger.getAnonymousLogger();
	{ m_logger.setLevel(Level.OFF); }
	
	public Database() {
		try {
			Class.forName(Config.getJDBCDriver());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load JDBC driver", e);
		}
		m_url = Config.getSQLServer() + ":user=" + Config.getUser() + ";password=" + Config.getPassword() + ";";
		m_props = new Properties();
		m_props.put("currentSchema", Config.getSQLSchema());
	}
	
	public int getOptimizerCalls() {
		return m_optimizerCalls;
	}
	
	public long getOptimizerTime() {
		return m_optimizerTime;
	}
	
	public int getCacheHits() {
		return m_cacheHits;
	}

	public double singleValueQuery(String query) throws WrappedSQLException {
		try {
			Statement stmt = getStatement();
			m_logger.info("Executing query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			ResultSetMetaData md = rs.getMetaData();
			assert(md.getColumnCount() == 1);
			assert(md.getColumnType(1) == Types.DOUBLE);
			if (rs.next()) {
				double value = rs.getDouble(1);
				assert(!rs.next());
				return value;
			}
			throw new SQLException("Didn't find a value!");
		} catch (SQLException e) {
			throw new WrappedSQLException(query, e);
		}
	}

	public ResultSet executeQuery(String query) throws WrappedSQLException {
		try {
			Statement stmt = getStatement();
			m_logger.info("Executing query: " + query);
			ResultSet rs = stmt.executeQuery(query);
			return rs;
		} catch (SQLException e) {
			// Add the query string to the exception for debugging purposes
			throw new WrappedSQLException(query, e);
		}
	}

	public void executeUpdate(String update) throws WrappedSQLException {
		try {
			Statement stmt = getStatement();
			m_logger.info("Executing update: " + update);
			int result = stmt.executeUpdate(update);
			m_logger.info("Result: " + result);
		} catch (SQLException e) {
			// Add the update string to the exception for debugging purposes
			throw new WrappedSQLException(update, e);
		}
	}

	public void dropTable(String s) throws WrappedSQLException {
		try {
			executeUpdate("drop table " + s);
			commit();
		} catch (WrappedSQLException e) {
			// ignore SQL state 42704 "An undefined object or constraint name was detected"
			Integer state = e.getSQLState();
			if (state == null || state != 42704) {
				throw(e);
			}
		}
	}
	
	protected void listAttributes(StringBuffer buf, int arity) {
		for (int i = 0; i < arity; i++) {
			if (i > 0) {
				buf.append(", ");
			}
			buf.append("A" + i);
		}
	}
	
	public int getCount(String s, int arity, int... values) throws SQLException, WrappedSQLException {
		assert(values.length <= arity+1);
		StringBuffer buf = new StringBuffer("select * \nfrom ");
		buf.append(s);
		buf.append("\nwhere ");
		for (int i = 0; i < values.length; i++) {
			if (i > 0) {
				buf.append(" and ");
			}
			if (i == arity) {
				buf.append("count");
			} else {
				buf.append("A");
				buf.append(i);
			}
			buf.append("=");
			buf.append(values[i]);
		}
		ResultSet result = executeQuery(buf.toString());
		int count = 0;
		while (result.next()) {
			count++;
		}
		return count;
	}
	
	public void insertTuple(String s, int... values) throws WrappedSQLException {
		StringBuffer buf = new StringBuffer("insert into ");
		buf.append(s);
		buf.append(" values (");
		for (int i = 0; i < values.length; i++) {
			if (i > 0) {
				buf.append(", ");
			}
			buf.append(values[i]);
		}
		buf.append(")");
		executeUpdate(buf.toString());
	}

	public void compactifyTable(String s, int arity) throws WrappedSQLException {
		// create new table
		String name = "scratch_" + s;
		createTable(name, arity);
		
		// populate the new table with entries of old table
		// but without duplicates (sum the count fields)
		StringBuffer buf = new StringBuffer("insert into ");
		buf.append(name);
		buf.append(" select ");
		listAttributes(buf, arity);
		buf.append(", sum(count)\n from ");
		buf.append(s);
		buf.append("\n group by ");
		listAttributes(buf, arity);
		buf.append("\n having sum(count) <> 0");
		executeUpdate(buf.toString());

		// drop old table and rename new table
		dropTable(s);
		renameTable(name, s);
	}

	public void renameTable(String from, String to) throws WrappedSQLException {
		executeUpdate("rename " + from + " to " + to);
	}
	
	public void createTable(String s, int arity) throws WrappedSQLException {
		StringBuffer buf = new StringBuffer("create table "); 
		buf.append(s);
		buf.append("(");
		for (int i = 0; i < arity; i++) {
			if (i > 0) {
				buf.append(", ");
			}
			buf.append("A");
			buf.append(i);
			buf.append(" integer");
		}
		buf.append(", count smallint not null default 1");
 		buf.append(") not logged initially");
		executeUpdate(buf.toString());
		commit();
	}
	
	public void runStats(String table) throws WrappedSQLException {
		String fullname = m_props.get("currentSchema") + "." + table;
		String update = "CALL SYSPROC.ADMIN_CMD('runstats on table " + fullname + "')";
		try {
//			Connection con = getConnection();
//			greg - no need for prepared statement here
//			CallableStatement cs = con.prepareCall(update);
//			cs.executeUpdate();
			m_statement.execute(update);
//			cs.close();
		} catch (SQLException e) {
			throw new WrappedSQLException(update, e);
		}
	}
	
	public Statistic[] createViews(Program p) throws WrappedSQLException {
		clearEstimates();
		Union[] views = p.getViews();
		Statistic[] stats = new Statistic[views.length];
		for (int i = 0; i < views.length; i++) {
			System.out.println("Creating view " + Utils.TOKENIZER.getString(views[i].getName()) + "...");
			resetView(views[i]);
			stats[i] = new Statistic();
			stats[i].origPlan = stats[i].optPlan = views[i];
			stats[i].origCost = stats[i].optCost = estimateCost(views[i], 0);
			stats[i].origExecuteTime = stats[i].optExecuteTime = executeView(views[i]);
		}
		return stats;
	}

	public long executeView(Union view) throws WrappedSQLException {
		String name = Utils.TOKENIZER.getString(view.getName());
		String update = "insert into " + name + " " + view.toSQL();
		long before = System.currentTimeMillis();
		executeUpdate(update);
		long after = System.currentTimeMillis();
		commit();
		runStats(name);
		return after-before;
	}

	public void resetView(Union view) throws WrappedSQLException {
		String name = Utils.TOKENIZER.getString(view.getName());
		dropTable(name);
		createTable(name, view.getArity());
	}
	
	private static final int CHUNKSIZE = 1024;
	
	public void fillRandom(String s, int arity, int keyStart, int number) throws WrappedSQLException {
		Random generator = new Random();
		for (int i = keyStart; i < keyStart+number; i+= CHUNKSIZE) {
			StringBuffer buf = new StringBuffer("insert into ");
			buf.append(s);
			buf.append("(");
			listAttributes(buf,arity);
			buf.append(") values ");
			for (int j = i; j < i+CHUNKSIZE && j < keyStart+number; j++) {
				if (j > i) {
					buf.append(",\n");
				}
				buf.append("(");
				buf.append(j);
				for (int k = 1; k < arity; k++) {
					buf.append(", ");
					buf.append(generator.nextInt());
				}
				buf.append(")");
			}
			executeUpdate(buf.toString());
			commit();
		}
		runStats(s);
	}
	
	public void fillChain(String s, int min, int max) throws WrappedSQLException {
		for (int i = min; i < max; i+= CHUNKSIZE) {
			StringBuffer buf = new StringBuffer("insert into ");
			buf.append(s);
			buf.append("(");
			listAttributes(buf,2);
			buf.append(") values ");
			for (int j = i; j < i+CHUNKSIZE && j < max; j++) {
				if (j > i) {
					buf.append(",\n");
				}
				buf.append("(");
				buf.append(j);
				buf.append(", ");
				buf.append(j+1);
				buf.append(")");
			}
			executeUpdate(buf.toString());
			commit();
		}
		runStats(s);
	}
	
	public void createAll(Schema s) throws WrappedSQLException {
		HashMap<Integer,Integer> map = s.getArities();
		for (int r : map.keySet()) {
			String name = Utils.TOKENIZER.getString(r);
			dropTable(name);
			createTable(name, map.get(r));
		}
	}
	
	public void dropAll(Schema s) throws WrappedSQLException {
		HashMap<Integer,Integer> map = s.getArities();
		for (int r : map.keySet()) {
			String name = Utils.TOKENIZER.getString(r);
			dropTable(name);
		}
	}
	
	protected Connection getConnection() throws SQLException {
		if (m_connection == null) {
			m_logger.info("Connecting to " + m_url + " using properties " + m_props);
			m_connection = DriverManager.getConnection(m_url, m_props);                                
			m_connection.setAutoCommit(false);
		}
		return m_connection;
	}
	
	public void commit() throws WrappedSQLException {
		try {
			getConnection().commit();
		} catch (SQLException e) {
			throw new WrappedSQLException("commit", e);
		}
	}
	
	public Statement getStatement() throws SQLException {
		if (m_statement == null) {
			Connection con = getConnection();
			con.commit();	// clear out the transaction log
			m_statement = con.createStatement();
		}
		return m_statement;
	}

	public void close() throws SQLException {
		if (m_connection != null) {
			m_connection.close();
			m_connection = null;
		}
		if (m_statement != null) {
			m_statement.close();
			m_statement = null;
		}
	}
	
	protected HashMap<SignedRule, Double> m_memo = new HashMap<SignedRule, Double>();
	protected SignedRule m_queryLast;
	protected Double m_costLast;
	
	public void clearCache() {
		m_memo.clear();
		m_queryLast = null;
		m_costLast = null;
	}
	
	public Double lookupCached(SignedRule union) {
		if (union != m_queryLast) {
			m_costLast = m_memo.get(union);
			m_queryLast = union;
		}
		if (m_costLast != null) {
			m_cacheHits++;
		}
		return m_costLast;
	}
	
	public void cache(SignedRule union, Double cost) {
		m_memo.put(union, cost);
		m_queryLast = union;
		m_costLast = cost;
	}
	
	public void clearEstimates() throws WrappedSQLException {
		String s = "delete from EXPLAIN_STATEMENT";
		executeUpdate(s);
		m_optimizerCalls = 0;
		m_optimizerTime = 0;
		m_cacheHits = 0;
	}
	
	public double estimateCost(Union union, int flags) throws WrappedSQLException {
		double total = 0.0;
		for (SignedRule rule : union.getRules()) {
			Double cost = lookupCached(rule);
			if (cost == null) {
				String query = rule.toSQL();
				cost = estimateCost(query);
				cache(rule,cost);
			}
			total += cost;
		}
		return total;
	}
	
	private double estimateCost(String query) throws WrappedSQLException {		
		m_logger.info("Estimating cost of (" + m_optimizerCalls + "): " + query);
		String s = "explain plan for snapshot set queryno = " + m_optimizerCalls + 
			" for " + query; 
		String s2 = "select TOTAL_COST from EXPLAIN_STATEMENT " + "" +
			"where QUERYNO = " + m_optimizerCalls + " and EXPLAIN_LEVEL = 'P'";
		m_optimizerCalls++;
		long before = System.currentTimeMillis();
		executeUpdate(s);
		double cost = singleValueQuery(s2);
		long after = System.currentTimeMillis();
		m_optimizerTime += after-before;
		return cost;
	}
	
	public void fillData(String data) throws WrappedSQLException {
		String[] lines = data.split("\n");
		for (String line : lines) {
			line = line.trim();
			if (!line.isEmpty()) {
				// S chain 0 10000
				String[] tokens = line.split("[ ]+");
				assert(tokens.length == 4 && tokens[1].equalsIgnoreCase("chain"));
				int from = Integer.parseInt(tokens[2]);
				int to = Integer.parseInt(tokens[3]);
				fillChain(tokens[0], from, to);
				commit();
			}
		}
	}

//	public static void main(String[] argv) {
//		try {                                                                        
//			String url = "jdbc:db2:SAMPLE";
//
//			// Load the DB2 Universal JDBC Driver
//			Class.forName("com.ibm.db2.jcc.DB2Driver");                              
//			System.out.println("**** Loaded the JDBC driver");
//
//			// Create the connection using the DB2 Universal JDBC Driver
//			Properties props = new Properties();
//			props.put("user", "db2admin");
//			props.put("password", "Need1Penn");
//			//props.put("currentSchema", "DB2ADMIN");
//			Connection con = DriverManager.getConnection(url, props);                                
//
//			// Commit changes manually
//			con.setAutoCommit(false);
//			System.out.println("**** Created a JDBC connection to the data source");
//
//			// Create the Statement
//			Statement stmt = con.createStatement();                                           
//			System.out.println("**** Created JDBC Statement object");
//
//			// Execute a query and generate a ResultSet instance
//			ResultSet rs = stmt.executeQuery("SELECT EMPNO FROM EMPLOYEE");                    
//			System.out.println("**** Creaed JDBC ResultSet object");
//
//			// Print all of the employee numbers to standard output device
//			while (rs.next()) {
//				String empNo = rs.getString(1);
//				System.out.println("Employee number = " + empNo);
//			}
//			System.out.println("**** Fetched all rows from JDBC ResultSet");
//
//			// Close the ResultSet
//			rs.close();
//			System.out.println("**** Closed JDBC ResultSet");
//
//			// Close the Statement
//			stmt.close();
//			System.out.println("**** Closed JDBC Statement");
//
//			// Connection must be on a unit-of-work boundary to allow close
//			con.commit();
//			System.out.println ( "**** Transaction committed" );
//
//			// Close the connection
//			con.close();                                                           
//			System.out.println("**** Disconnected from data source");
//
//			System.out.println("**** JDBC Exit from class EzJava - no errors");
//
//		} catch (ClassNotFoundException e) {
//			System.err.println("Could not load JDBC driver");
//			System.out.println("Exception: " + e);
//			e.printStackTrace();
//
//		} catch(SQLException ex) {
//		
//			System.err.println("SQLException information");
//			while(ex != null) {
//				System.err.println ("Error msg: " + ex.getMessage());
//				System.err.println ("SQLSTATE: " + ex.getSQLState());
//				System.err.println ("Error code: " + ex.getErrorCode());
//				ex.printStackTrace();
//				ex = ex.getNextException(); // For drivers that support chained exceptions
//			}
//		}
//	}
}
