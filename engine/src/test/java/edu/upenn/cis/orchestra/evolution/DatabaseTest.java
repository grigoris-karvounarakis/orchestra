package edu.upenn.cis.orchestra.evolution;

import java.sql.SQLException;

import junit.framework.Assert;
import junit.framework.TestCase;


public class DatabaseTest extends TestCase {
	public void testCompactify() throws WrappedSQLException, SQLException {
		System.out.println("Setting up compactify test...");
		Database db = new Database();
		db.dropTable("R");
		db.createTable("R", 1);

		// insert some (non-compactified) data
		db.insertTuple("R", 1, 1);
		db.insertTuple("R", 1, 1);
		db.insertTuple("R", 2, 2);
		db.insertTuple("R", 2, -1);
		db.insertTuple("R", 2, -1);
		db.insertTuple("R", 3, -2);
		db.insertTuple("R", 3, 1);
		db.commit();
		
		// compactify the table
		System.out.println("Compactifying...");
		db.compactifyTable("R", 1);
		db.commit();

		// verify the resulting table
		System.out.println("Verifying result...");
		Assert.assertEquals(1, db.getCount("R", 1, 1, 2));
		Assert.assertEquals(1, db.getCount("R", 1, 1));
		Assert.assertEquals(0, db.getCount("R", 1, 2));
		Assert.assertEquals(1, db.getCount("R", 1, 3, -1));
		Assert.assertEquals(1, db.getCount("R", 1, 3));

		// clean up
		db.dropTable("R");
		System.out.println("Passed");
	}
}
