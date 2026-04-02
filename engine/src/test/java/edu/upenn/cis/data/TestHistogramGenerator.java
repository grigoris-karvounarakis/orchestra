package edu.upenn.cis.data;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.optimization.Histogram;
import edu.upenn.cis.orchestra.optimization.Histogram.Results;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.SimpleTableNameGenerator;
import edu.upenn.cis.orchestra.p2pqp.TableNameGenerator;

public class TestHistogramGenerator {
	QpSchema schema;
	List<QpTuple<?>> tuples = new ArrayList<QpTuple<?>>();
	Results zero, one, two;
	File f;
	Environment e;
	TableNameGenerator tng;

	@Before
	public void setUp() throws Exception {
		schema = new QpSchema("R", 0);
		schema.addCol("x", IntType.INT);
		schema.addCol("y", new StringType(false,false,true, 5));
		schema.markFinished();
		tuples.add(new QpTuple<Null>(schema, new Object[] {1, "Hello"}));
		tuples.add(new QpTuple<Null>(schema, new Object[] {5, "Hello"}));
		tuples.add(new QpTuple<Null>(schema, new Object[] {9, "Bye"}));
		one = new Results(1.0, 1.0);
		two = new Results(2.0, 1.0);
		zero = new Results(0.0, 0.0);
		f = new File("histEnv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		e = new Environment(f, ec);
		tng = new SimpleTableNameGenerator("Histogram");
	}

	@After
	public void tearDown() throws Exception {
		e.close();
		File[] files = f.listFiles();
		for (File file : files) {
			file.delete();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCompleteHistogram() throws DatabaseException {
		List<Histogram<?>> hists = HistogramGenerator.generateHistograms(e, tng, schema, tuples.iterator(), 10);

		Histogram<Integer> xHist = (Histogram<Integer>) hists.get(0);
		assertEquals(one, xHist.getNumInRange(1, 1));
		assertEquals(one, xHist.getNumInRange(5, 5));
		assertEquals(one, xHist.getNumInRange(9, 9));
		assertEquals(zero, xHist.getNumInRange(null, 0));
		assertEquals(zero, xHist.getNumInRange(2, 4));
		assertEquals(zero, xHist.getNumInRange(6, 8));
		assertEquals(zero, xHist.getNumInRange(10, null));

		Histogram<String> yHist = (Histogram<String>) hists.get(1);
		assertEquals(two, yHist.getNumInRange("Hello", "Hello"));
		assertEquals(one, yHist.getNumInRange("Bye", "Bye"));
	}

}
