package edu.upenn.cis.orchestra.p2pqp;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.p2pqp.SpillingTupleCollection.FullTuples;
import edu.upenn.cis.orchestra.p2pqp.SpillingTupleCollection.KeyViolation;

public class TestSpillingTupleCollection {
	QpSchema r;
	QpTuple<Null> r1, r2, r3;
	private static ScratchFileGenerator sfg;
	private static File f = new File("scratch");
	
	@BeforeClass
	public static void createTempSpace() {
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		sfg = new SimpleScratchFileGenerator(f,"temp");		
	}
	
	@AfterClass
	public static void removeTempSpace() {
		System.gc();
		f.deleteOnExit();
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.deleteOnExit();
			}
		}
	}
	
	@Before
	public void setUp() throws Exception {
		r = new QpSchema("R",1);
		r.addCol("a", new IntType(false,false));
		r.addCol("b", new StringType(false, false, true, 10));
		r.setPrimaryKey(Arrays.asList("a"));
		r.setHashCols(new int[] {0});
		r.markFinished();
		
		r1 = new QpTuple<Null>(r, new Object[] {1, "Nick"});
		r2 = new QpTuple<Null>(r, new Object[] {2, "Fred"});
		r3 = new QpTuple<Null>(r, new Object[] {3, "Dan"});	
	}
	
	@Test
	public void testCreate() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		assertTrue(ts.isEmpty());
	}

	@Test
	public void testSize() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		
		ts.add(r1);
		ts.add(r2);
		ts.add(r3);
		assertEquals("Incorrect size of SpillingTupleCollection",3,ts.size());
		assertFalse(ts.isEmpty());
	}
	
	@Test
	public void testSimpleSort() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		
		ts.add(r1);
		Iterator<FullTuples<Null>> it = ts.iterator();
		assertTrue(it.hasNext());
		FullTuples<Null> ft = it.next();
		assertFalse(it.hasNext());
		
		assertEquals(ft.id, r1.getQPid());
		assertEquals(Collections.singletonList(r1), ft.tuples);
	}
	
	@Test
	public void testLargerSort() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		
		ts.add(r1);
		ts.add(r2);
		ts.add(r3);
		Iterator<FullTuples<Null>> it = ts.iterator();
		List<QpTuple<Null>> found = new ArrayList<QpTuple<Null>>();
		List<QpTuple<Null>> expected = new ArrayList<QpTuple<Null>>();
		expected.add(r1);
		expected.add(r2);
		expected.add(r3);
		Collections.sort(expected, byQPid);
		while (it.hasNext()) {
			found.addAll(it.next().tuples);
		}
		assertEquals(expected, found);
		
	}
	
	@Test
	public void testMerge() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		
		ts.add(r1);
		ts.iterator();
		ts.add(r2);
		Iterator<FullTuples<Null>> it = ts.iterator();
		
		List<QpTuple<Null>> found = new ArrayList<QpTuple<Null>>();
		List<QpTuple<Null>> expected = new ArrayList<QpTuple<Null>>();
		expected.add(r1);
		expected.add(r2);
		Collections.sort(expected, byQPid);
		while (it.hasNext()) {
			found.addAll(it.next().tuples);
		}
		assertEquals(expected, found);
	}
	
	@Test(expected=KeyViolation.class)
	public void testKeyViolation() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		
		ts.add(r1);
		ts.iterator();
		ts.add(r1);
		ts.iterator();
	}
	
	@Test(expected=KeyViolation.class)
	public void testKeyViolationNoMerge() throws Exception {
		SpillingTupleCollection<Null> ts = new SpillingTupleCollection<Null>(sfg, r);
		
		ts.add(r1);
		ts.add(r1);
		ts.iterator();
	}

	private static final Comparator<QpTuple<?>> byQPid = new Comparator<QpTuple<?>>() {

		@Override
		public int compare(QpTuple<?> arg0, QpTuple<?> arg1) {
			return arg0.getQPid().compareTo(arg1.getQPid());
		}
		
	};
}
