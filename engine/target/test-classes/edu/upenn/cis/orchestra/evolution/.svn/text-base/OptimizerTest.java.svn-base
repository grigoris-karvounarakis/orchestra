package edu.upenn.cis.orchestra.evolution;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.HashMap;

import junit.framework.Assert;
import junit.framework.TestCase;

public class OptimizerTest extends TestCase {

	static public void main(String[] args) throws Exception {
		OptimizerTest opt = new OptimizerTest();
		opt.testSimpleOptimizer();
	}
	
	public void testSimpleOptimizer() throws WrappedSQLException, SQLException, IOException {
		testSimple("SimpleOptimizerTest.txt");
	}

	public void UtestDeltaRules() throws WrappedSQLException, SQLException, IOException {
		testSimple("DeltaRulesTest.txt");
	}

	private void testSimple(String filename) throws WrappedSQLException, SQLException, IOException {
		InputStream input = OptimizerTest.class.getResourceAsStream(filename);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		Database db = new Database();
		int count = 1;
		while (Utils.skipBlanks(reader)) {
			HashMap<String, String> blocks = Utils.readBlocks(reader); 
			String test = blocks.get("TEST");
			int flags = Optimizer.Flags.parse(blocks.get("FLAGS"));
			Assert.assertTrue(flags != 0);
			Schema schema = Schema.parse(blocks.get("SCHEMA"));
			String data = blocks.get("DATA");
			Program views = Program.parse(blocks.get("VIEWS"));
			Union query = Union.parse(blocks.get("QUERY"));
			views = views.unfoldAll();
			query = views.unfoldQuery(query);
			Union expected = Union.parse(blocks.get("EXPECTED"));
			System.out.println("Setting up simple test case " + count++ + " \"" + test + "\" (" + Optimizer.Flags.toString(flags) + ")...");
			db.clearEstimates();
			db.createAll(schema);
			db.fillData(data);
			db.createViews(views);
			db.commit();
			System.out.println("Optimizing and updating...");
			Optimizer opt = new Optimizer(db);
			Statistic stat = opt.optimizedUpdate(views, query, flags);
			System.out.println("Expected plan: " + expected);
			System.out.println("Estimated cost: " + db.estimateCost(expected, flags));
			System.out.println(stat.toString());
			db.dropAll(schema);
			db.commit();
			boolean passed = stat.optPlan.findIsomorphism(expected) != null;
			if (passed) {
				System.out.println("PASSED\n");
			} else {
				System.out.println("FAILED\n");
			}
			Assert.assertTrue(passed);
		}
		db.close();
	}

	public void testCompound() throws WrappedSQLException, SQLException, IOException {
		InputStream input = OptimizerTest.class.getResourceAsStream("CompoundOptimizerTest.txt");
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		Database db = new Database();
		int count = 1;
		while (Utils.skipBlanks(reader)) {
			HashMap<String, String> blocks = Utils.readBlocks(reader); 
			String test = blocks.get("TEST");
			int flags = Optimizer.Flags.parse(blocks.get("FLAGS"));
			Assert.assertTrue(flags != 0);
			Schema schema = Schema.parse(blocks.get("SCHEMA"));
			String data = blocks.get("DATA");
			Program before = Program.parse(blocks.get("OLD"));
			Program after = Program.parse(blocks.get("NEW"));
			System.out.println("Setting up compound test case " + count++ + " \"" + test + "\" (" + Optimizer.Flags.toString(flags) + ")...");
			db.clearEstimates();
			db.createAll(schema);
			db.fillData(data);
			db.createViews(before.unfoldAll());
			db.createViews(after.unfoldAll());
			db.commit();
			System.out.println("Optimizing and updating...");
			Optimizer opt = new Optimizer(db);
			Statistic[] stats = opt.optimizedUpdate(before, after, flags);
			for (int i = 0; i < stats.length; i++) {
				System.out.println("\nStep " + i + "");
				System.out.println(stats[i].toString());
			}
			System.out.println("\nDone.\n\n");
			db.dropAll(schema);
			db.commit();
		}
		db.close();
	}	
}
