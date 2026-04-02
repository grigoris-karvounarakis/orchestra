import java.net.*;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import COM.ibm.db2.app.*;
import java.sql.Date;

public class Skolem extends COM.ibm.db2.app.UDF {
	public static class SkolemParameters {
		public SkolemParameters() {
			setFnName("");
			setAttribNames(new ArrayList<String>());
			setTypeDefs(new ArrayList<String>());
			setJavaTypes(new ArrayList<String>());
		}
		public SkolemParameters(SkolemParameters other) {
			setFnName(other.getFnName());
			setAttribNames(new ArrayList<String>());
			setTypeDefs(new ArrayList<String>());
			setJavaTypes(new ArrayList<String>());

			getAttribNames().addAll(other.getAttribNames());
			getTypeDefs().addAll(other.getTypeDefs());
			getJavaTypes().addAll(other.getJavaTypes());
		}
		public void add(String aType, String attr, String typedef, String javaType) {
			setFnName(getFnName() + aType);
			getAttribNames().add(attr);
			getTypeDefs().add(typedef);
			getJavaTypes().add(javaType);
		}
		public int getNumAttribs() {
			return getAttribNames().size();
		}
		public void setAttribNames(ArrayList<String> attribNames) {
			_attribNames = attribNames;
		}
		public ArrayList<String> getAttribNames() {
			return _attribNames;
		}
		public String getAttribNameAt(int i) {
			return _attribNames.get(i);
		}
		public void setTypeDefs(ArrayList<String> typeDefs) {
			_typeDefs = typeDefs;
		}
		public ArrayList<String> getTypeDefs() {
			return _typeDefs;
		}
		public String getTypeDefAt(int i) {
			return _typeDefs.get(i);
		}
		public String getTypeDefNNAt(int i) {
			return _typeDefs.get(i) + " NOT NULL";
		}
		public void setFnName(String fnName) {
			_fnName = fnName;
		}
		public String getFnName() {
			return _fnName;
		}
		public void setJavaTypes(ArrayList<String> javaTypes) {
			_javaTypes = javaTypes;
		}
		public ArrayList<String> getJavaTypes() {
			return _javaTypes;
		}
		public String getJavaTypeAt(int i) {
			return _javaTypes.get(i);
		}
		public String getJavaSetter(int i) {
			String s = _javaTypes.get(i);
			if (s.equals("int"))
				return "setInt";
			else
				return "set" + s;
		}
		private String _fnName;
		private ArrayList<String> _attribNames;
		private ArrayList<String> _typeDefs;
		private ArrayList<String> _javaTypes;
	}
	private static int _newSkVal = -2;
	public static String CONNECTION = "/skolem";
	private static boolean isActive = false;

	private static PrintWriter log;
	static enum AType {STR};
	private static Connection db;

	private static synchronized int getNextSkolem() {
		return _newSkVal--;
	}

	public Skolem() {
		super();

		try {
			connect();
		} catch (SQLException se) {
				se.printStackTrace(log);
		} catch (IOException ie) {
				ie.printStackTrace(log);
		}
	}
	public static boolean connect() throws SQLException, IOException {
		if (!isActive) {
		log = new PrintWriter(new FileWriter("skolems.log", true), true);
			log.println("Server daemon initializing...");
			try {
				Class.forName ("org.hsqldb.jdbcDriver");
			} catch (ClassNotFoundException cnf) {
				cnf.printStackTrace(log);
				System.exit(1);
			}
			String conn = "jdbc:hsqldb:file:" + CONNECTION;
			db = DriverManager.getConnection(conn);

			try {
				BufferedReader f = new BufferedReader(new FileReader("/cygwin/home/gkarvoun/EclipseWorkspace/sharq/experiments/tests/last.val"));

				String str = f.readLine();

				_newSkVal = Integer.valueOf(str);
				f.close();
			} catch (FileNotFoundException fne) {
				// Skip if no file
				try {
					createTables(20);
				} catch (SQLException se) {
					isActive = false;
					log.println("Error " + se.getMessage());
					se.printStackTrace(log);
					System.exit(1);
					}
				}
				isActive = true;
			}
			return isActive;
		}
	public void close() {
		log.println("Closing");
		try {
			db.close();
		} catch (SQLException e) {
			e.printStackTrace(log);
		}
			db = null;
			isActive = false;
	}
	public boolean isEnabled() {
		return isActive;
	}
	private static SkolemParameters getAttribDDL(SkolemParameters existingString, AType typ, int curDepth) {
		SkolemParameters ret = new SkolemParameters(existingString);
		if (typ == AType.STR) {
			ret.add("Str", "StrVal" + Integer.toString(curDepth), "VARCHAR(128)", "String");
		} else {
			log.println("DDL req for unknonw type");
			throw new RuntimeException("Illegal type");
		}
		return ret;
	}
	private static ArrayList<SkolemParameters> getAttribStatements(ArrayList<SkolemParameters> aList, int curDepth, int maxDepth) {
	if (curDepth == maxDepth)
		return new ArrayList<SkolemParameters>();
	ArrayList<SkolemParameters> ret = new ArrayList<SkolemParameters>();
	for (SkolemParameters a : aList) {
		ret.add(getAttribDDL(a, AType.STR, curDepth));
	}
	ret.addAll(getAttribStatements(ret, curDepth + 1, maxDepth));
	return ret;
}
	public static synchronized int skolem(String nam, Object[] parms) throws SQLException, IOException {
		int skVal;
		connect();
		String stmt = "SELECT * FROM Skolems." + nam + " WHERE Func = ?";

log.print(parms[0].toString() + "(");
		int pos = 0;
		for (int i = 1; i < parms.length; i++) {
			if (i > 1)
				log.print(",");
				if (parms[i] != null) {
					log.print(parms[i].toString());
		}
			stmt = stmt + " AND ";

			stmt = stmt + nam.substring(pos, pos + 3);
			pos += 3;
			stmt = stmt + "Val" + String.valueOf(i - 1) + " = ?";
		}
log.println(")");
		PreparedStatement ps = db.prepareStatement(stmt);
		for (int i = 0; i < parms.length; i++)
			if (parms[i] instanceof String)
				ps.setString(i+1, (String)parms[i]);
			else if (parms[i] instanceof Integer)
				ps.setInt(i+1, ((Integer)parms[i]).intValue());
			else if (parms[i] instanceof Date)
				ps.setDate(i+1, (Date)parms[i]);

			ResultSet rs = ps.executeQuery();

			if (rs.next()) {
				skVal = rs.getInt(parms.length+1);
			} else {
				skVal = getNextSkolem();

				String upd = "INSERT INTO Skolems." + nam + " VALUES (?";
				for (int i = 0; i < parms.length; i++)
					upd = upd + ",?";
				upd = upd + ")";

				ps = db.prepareStatement(upd);
				for (int i = 0; i < parms.length; i++)
					if (parms[i] instanceof String)
						ps.setString(i+1, (String)parms[i]);
					else if (parms[i] instanceof Integer)
						ps.setInt(i+1, ((Integer)parms[i]).intValue());
					else if (parms[i] instanceof Date)
						ps.setDate(i+1, (Date)parms[i]);

				ps.setInt(parms.length+1, skVal);
				ps.executeUpdate();
				if (ps != null) ps.close();
			}
		if (rs != null) rs.close();
		if (ps != null) ps.close();
		return skVal;
	}
	public static void createTables(int depth) throws SQLException {
		ArrayList<SkolemParameters> statements = new ArrayList<SkolemParameters>();
		statements.add(new SkolemParameters());

		statements = getAttribStatements(statements, 0, depth);

		createTableDDL(statements);
	}
	public static void createTableDDL(ArrayList<SkolemParameters> statements) throws SQLException {
		Statement st = db.createStatement();
		try {
			st.execute("CREATE SCHEMA Skolems AUTHORIZATION DBA");
		} catch (SQLException schemaE) {

		}
		for (SkolemParameters s : statements) {
			String statement = "CREATE CACHED TABLE Skolems." + s.getFnName() + "(Func VARCHAR(10) NOT NULL";
			String pKey = "PRIMARY KEY (Func";
			for (int i = 0; i < s.getNumAttribs(); i++) {
				statement = statement + ", " + s.getAttribNameAt(i) + " " + s.getTypeDefNNAt(i);
				pKey = pKey + ", " + s.getAttribNameAt(i);
			}
			statement = statement + ", SkVal INTEGER";
			statement = statement + ", " + pKey + "));";
			log.println("Creating " + s.getFnName());
			try {
				st.execute(statement);
			} catch (SQLException schemaE) {
				schemaE.printStackTrace(log);
			}
		}
		st.close();
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStr(String skolemName, String StrVal0) throws IOException {
		Object[] parms = new Object[2];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		try {
			int skVal = skolem("Str", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStr(String skolemName, String StrVal0, String StrVal1) throws IOException {
		Object[] parms = new Object[3];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		try {
			int skVal = skolem("StrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2) throws IOException {
		Object[] parms = new Object[4];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		try {
			int skVal = skolem("StrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3) throws IOException {
		Object[] parms = new Object[5];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		try {
			int skVal = skolem("StrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4) throws IOException {
		Object[] parms = new Object[6];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		try {
			int skVal = skolem("StrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5) throws IOException {
		Object[] parms = new Object[7];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		try {
			int skVal = skolem("StrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6) throws IOException {
		Object[] parms = new Object[8];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		try {
			int skVal = skolem("StrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7) throws IOException {
		Object[] parms = new Object[9];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8) throws IOException {
		Object[] parms = new Object[10];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9) throws IOException {
		Object[] parms = new Object[11];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10) throws IOException {
		Object[] parms = new Object[12];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11) throws IOException {
		Object[] parms = new Object[13];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12) throws IOException {
		Object[] parms = new Object[14];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13) throws IOException {
		Object[] parms = new Object[15];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14) throws IOException {
		Object[] parms = new Object[16];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		parms[15] = StrVal14;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14, String StrVal15) throws IOException {
		Object[] parms = new Object[17];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		parms[15] = StrVal14;
		parms[16] = StrVal15;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14, String StrVal15, String StrVal16) throws IOException {
		Object[] parms = new Object[18];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		parms[15] = StrVal14;
		parms[16] = StrVal15;
		parms[17] = StrVal16;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14, String StrVal15, String StrVal16, String StrVal17) throws IOException {
		Object[] parms = new Object[19];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		parms[15] = StrVal14;
		parms[16] = StrVal15;
		parms[17] = StrVal16;
		parms[18] = StrVal17;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14, String StrVal15, String StrVal16, String StrVal17, String StrVal18) throws IOException {
		Object[] parms = new Object[20];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		parms[15] = StrVal14;
		parms[16] = StrVal15;
		parms[17] = StrVal16;
		parms[18] = StrVal17;
		parms[19] = StrVal18;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14, String StrVal15, String StrVal16, String StrVal17, String StrVal18, String StrVal19) throws IOException {
		Object[] parms = new Object[21];

		parms[0] = skolemName;
		parms[1] = StrVal0;
		parms[2] = StrVal1;
		parms[3] = StrVal2;
		parms[4] = StrVal3;
		parms[5] = StrVal4;
		parms[6] = StrVal5;
		parms[7] = StrVal6;
		parms[8] = StrVal7;
		parms[9] = StrVal8;
		parms[10] = StrVal9;
		parms[11] = StrVal10;
		parms[12] = StrVal11;
		parms[13] = StrVal12;
		parms[14] = StrVal13;
		parms[15] = StrVal14;
		parms[16] = StrVal15;
		parms[17] = StrVal16;
		parms[18] = StrVal17;
		parms[19] = StrVal18;
		parms[20] = StrVal19;
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", parms);
			log.println("Returned " + skVal);
			return skVal;
		} catch (SQLException ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
}
