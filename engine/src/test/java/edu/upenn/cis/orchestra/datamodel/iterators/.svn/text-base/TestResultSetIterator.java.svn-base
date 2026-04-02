package edu.upenn.cis.orchestra.datamodel.iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestResultSetIterator {
	static final String jdbcUrl = "jdbc:db2://grw561-3.cis.upenn.edu:50000/orchestr";
	static final String username = "orchestra";
	static final String password = "apollo";
	static Connection conn;
	static Statement s;

	private static class It extends ResultSetIterator<Integer> {

		public It(ResultSet rs) throws SQLException {
			super(rs);
		}

		@Override
		public Integer readCurrent() throws IteratorException {
			try {
				return rs.getInt(1);
			} catch (SQLException e) {
				throw new IteratorException(e);
			}
		}

	}

	@BeforeClass
	public static void setUp() throws Exception {
		Class.forName("com.ibm.db2.jcc.DB2Driver");
		conn = DriverManager.getConnection(jdbcUrl, username, password);
		s = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		try {
			s.execute("DROP TABLE testresultsetiterator");
		} catch (SQLException e) {
			// Table already exists;
		}
		s.execute("CREATE TABLE testresultsetiterator(a INTEGER NOT NULL PRIMARY KEY)");
	}

	@Before
	public void clear() throws Exception {
		s.execute("DELETE FROM testresultsetiterator");
	}

	@AfterClass
	public static void tearDown() throws Exception {
		s.execute("DROP TABLE testresultsetiterator");
		s.close();
		conn.close();
	}

	@Test(expected=NoSuchElementException.class)
	public void empty() throws Exception {
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator");
		It it = new It(rs);
		try {

			assertFalse("Empty result set should not have a previous element", it.hasPrev());
			assertFalse("Empty result set should not have a next element", it.hasNext());
			it.next();
		} finally {
			it.close();
		}
	}

	@Test
	public void forwards() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1,2,3");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			assertFalse("First row should not have a previous element", it.hasPrev());
			assertTrue("Result set should have a next element", it.hasNext());
			assertEquals("Incorrect first result from iterator", 1, it.next());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect second result from iterator", 2, it.next());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect third result from iterator", 3, it.next());
			assertFalse("Last row should not have a next element", it.hasNext());
			assertTrue("Last row should have a previous element", it.hasPrev());
		} finally {
			it.close();
		}
	}

	@Test
	public void backwards() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1,2,3");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			while (it.hasNext()) {
				it.next();
			}
			assertTrue("Last row should have a previous element", it.hasPrev());
			assertFalse("Last row should not have a next element", it.hasNext());
			assertEquals("Incorrect third result from iterator", 3, it.prev());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect second result from iterator", 2, it.prev());
			assertTrue("Result set should have a next element", it.hasNext());
			assertTrue("Result set should have a previous element", it.hasPrev());
			assertEquals("Incorrect first result from iterator", 1, it.prev());
			assertFalse("First row should not have a previous element", it.hasPrev());
			assertTrue("Result set should have a next element", it.hasNext());
		} finally {
			it.close();
		}
	}

	@Test
	public void backAndForthOneElement() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			assertEquals("Incorrect result from first forwards", 1, it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertEquals("Incorrect result from first backwards", 1, it.prev());

			assertTrue("Before first row should have a next element", it.hasNext());
			assertFalse("Before first row should not have a previous element", it.hasPrev());

			assertEquals("Incorrect result from second forwards", 1, it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertTrue("After last row should have a previous element", it.hasPrev());
			assertEquals("Incorrect result from second backwards", 1, it.prev());
		} finally {
			it.close();
		}
	}

	@Test
	public void backAndForthTwoElements() throws Exception {
		s.executeUpdate("INSERT INTO testresultsetiterator VALUES 1,2");
		ResultSet rs = s.executeQuery("SELECT * FROM testresultsetiterator ORDER BY a");
		It it = new It(rs);
		try {
			assertEquals("Incorrect result from first forwards", 1, it.next());
			assertEquals("Incorrect result from first forwards", 2, it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertEquals("Incorrect result from first backwards", 2, it.prev());
			assertEquals("Incorrect result from first backwards", 1, it.prev());

			assertTrue("Before first row should have a next element", it.hasNext());
			assertFalse("Before first row should not have a previous element", it.hasPrev());

			assertEquals("Incorrect result from second forwards", 1, it.next());
			assertEquals("Incorrect result from second forwards", 2, it.next());
			assertFalse("After last row should not have a next element", it.hasNext());
			assertTrue("After last row should have a previous element", it.hasPrev());
			assertEquals("Incorrect result from second backwards", 2, it.prev());
			assertEquals("Incorrect result from second backwards", 1, it.prev());
		} finally {
			it.close();
		}
	}
}
