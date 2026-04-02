import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Date;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;

public class SkolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr extends COM.ibm.db2.app.UDF {
	private static int FIRST_SKOLEM = -2040109460;
	public static String CONNECTION = "skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr";
	private static final byte INT = 1, STRING = 2, DATE = 3, DOUBLE = 4;
	private static boolean isActive = false;

	private static PrintWriter log;
	private static Database _db;
	private static Environment _env;

	private static synchronized int getNextSkolem() throws DatabaseException {
		DatabaseEntry key = new DatabaseEntry(new String("##SKVAL##").getBytes()), value = new DatabaseEntry();
		OperationStatus os = _db.get(null, key, value, null);
		int retval;
		if (os == OperationStatus.NOTFOUND) {
			retval = FIRST_SKOLEM;
		} else {
			retval = getValFromBytes(value.getData()) - 1;
		}
		value.setData(getBytes(retval));
		_db.put(null, key, value);
		return retval;
	}

	public SkolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr() {
		super();
		try {
			log = new PrintWriter(new FileWriter("skolems.log", true), true);
			log.println("Server daemon initializing...");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static boolean connect() throws DatabaseException, IOException {
		if (!isActive) {
			try {
				log = new PrintWriter(new FileWriter("skolems.log", true), true);
				EnvironmentConfig envCfg = new EnvironmentConfig();
				envCfg.setAllowCreate(true);
				envCfg.setTransactional(false);
				File file = new File(CONNECTION);
				file.mkdir();
				_env = new Environment(file, envCfg);
				DatabaseConfig databaseConfig = new DatabaseConfig();
				databaseConfig.setAllowCreate(true);
				databaseConfig.setSortedDuplicates(false);

				_db = _env.openDatabase(null, CONNECTION + File.separator + CONNECTION, databaseConfig);
			} catch (DatabaseException dnf) {
				dnf.printStackTrace(log);
				throw dnf;
			}

			isActive = true;
		}
		return isActive;
	}

	public static void closeDown() {
		log.println("Closing");
		try {
			_db.close();
			_env.close();
		} catch (DatabaseException e) {
			e.printStackTrace(log);
		}
		isActive = false;
	}

	public static synchronized int skolem(String function, Object... args) {
		byte[] keyBytes;
		try {
			connect();
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			ObjectOutputStream out = new ObjectOutputStream(bytes);
			byte[] funcBytes = function.getBytes("UTF-8");
			out.writeInt(funcBytes.length);
			out.write(funcBytes);
			for (Object o : args) {
				if (o instanceof Integer) {
					out.writeByte(INT);
				} else if (o instanceof String) {
					out.writeByte(STRING);
				} else if (o instanceof Double) {
					out.writeByte(DOUBLE);
				} else if (o instanceof Date) {
					out.writeByte(DATE);
				} else {
					throw new RuntimeException("Don't know what to do with object " + o + " of type " + o.getClass().getCanonicalName());
				}
			}
					for (Object o : args) {
				if (o instanceof Integer) {
					out.writeInt((Integer) o);
				} else if (o instanceof String) {
					byte[] strBytes = ((String) o).getBytes("UTF-8");
					out.writeInt(strBytes.length);
					out.write(strBytes);
				} else if (o instanceof Double) {
					out.writeDouble((Double) o);
				} else if (o instanceof Date) {
					out.writeLong(((Date) o).getTime());
				} else {
					throw new RuntimeException("Don't know what to do with object " + o + " of type " + o.getClass().getCanonicalName());
				}
			}
			out.flush();
			keyBytes = bytes.toByteArray();
		} catch (IOException ioe) {
			ioe.printStackTrace(log);
			throw new RuntimeException(ioe);
		} catch (DatabaseException dbe) {
			dbe.printStackTrace(log);
			throw new RuntimeException(dbe);
		}
		DatabaseEntry key = new DatabaseEntry(keyBytes), value = new DatabaseEntry();
		try {
			OperationStatus os = _db.get(null, key, value, null);
			if (os == OperationStatus.SUCCESS) {
				closeDown();
				return getValFromBytes(value.getData());
			} else {
				int retval = getNextSkolem();
				value.setData(getBytes(retval));
				_db.put(null, key, value);
				closeDown();
				return retval;
			}
		} catch (DatabaseException de) {
			throw new RuntimeException(de);
		}
	}
	public static byte[] getBytes(int value) {
		byte[] retval = new byte[4];
		for (int i = 0; i < 4; ++i) {
			retval[i] = (byte) (value >>> ((4 - i - 1) * 8));
		}
		return retval;
	}
	public static int getValFromBytes(byte[] bytes) {
		if (bytes.length != 4) {
			throw new RuntimeException("Byteified integers must have length 4");
		}
		int value = 0;
		for (int i = 0; i < 4; ++i) {
			value |= ((int) (bytes[i] & 0xFF)) << ((4 - i - 1) * 8);
		}
		return value;
	}
/* This code is automatically generated */
	/**
	 * Compute skolem function
	 */
	public static int skolemStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr(String skolemName, String StrVal0, String StrVal1, String StrVal2, String StrVal3, String StrVal4, String StrVal5, String StrVal6, String StrVal7, String StrVal8, String StrVal9, String StrVal10, String StrVal11, String StrVal12, String StrVal13, String StrVal14, String StrVal15, String StrVal16, String StrVal17, String StrVal18, String StrVal19) throws IOException {
		try {
			int skVal = skolem("StrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStrStr", skolemName, StrVal0, StrVal1, StrVal2, StrVal3, StrVal4, StrVal5, StrVal6, StrVal7, StrVal8, StrVal9, StrVal10, StrVal11, StrVal12, StrVal13, StrVal14, StrVal15, StrVal16, StrVal17, StrVal18, StrVal19);
			log.println("Returned " + skVal);
			return skVal;
		} catch (Exception ce) {
			ce.printStackTrace(log);
			return 0;
		}
	}
}
