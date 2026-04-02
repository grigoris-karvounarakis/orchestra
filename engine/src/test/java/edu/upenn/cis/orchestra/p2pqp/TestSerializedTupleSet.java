package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.StringType;

public class TestSerializedTupleSet {
	private QpSchema r;
	private QpTuple<?> rNick11, rNick13, rNick22, rNick24, rNick35;
	private QpTuple<?> rJim23, rJim15, rJim57;
	private QpTupleKey rNick11_1, rNick13_2, rNick22_2, rNick24_1, rNick35_1;
	private QpTupleKey rJim23_1, rJim15_1, rJim57_1;
	private SerializedTupleSet set;
	
	
	@Before
	public void setUp() throws Exception {
		r = new QpSchema("R",1);
		r.addCol("a", new StringType(false,false,true,10));
		r.addCol("b", new IntType(false,false));
		r.addCol("c", new IntType(false,false));
		r.setPrimaryKey(Arrays.asList("a", "b"));
		r.setHashCols(Collections.singleton("a"));
		r.markFinished();
		
		rNick11 = new QpTuple<Null>(r, new Object[] {"Nick", 1, 1});
		rNick11_1 = rNick11.getKeyTuple(1);
		rNick22 = new QpTuple<Null>(r, new Object[] {"Nick", 2, 2});
		rNick22_2 = rNick22.getKeyTuple(2);
		rNick13 = new QpTuple<Null>(r, new Object[] {"Nick", 1, 3});
		rNick13_2 = rNick13.getKeyTuple(2);
		rNick24 = new QpTuple<Null>(r, new Object[] {"Nick", 2, 4});
		rNick24_1 = rNick24.getKeyTuple(1);
		rNick35 = new QpTuple<Null>(r, new Object[] {"Nick", 3, 5});
		rNick35_1 = rNick35.getKeyTuple(1);
		
		rJim23 = new QpTuple<Null>(r, new Object[] {"Jim", 2, 3});
		rJim23_1 = rJim23.getKeyTuple(1);
		rJim15 = new QpTuple<Null>(r, new Object[] {"Jim", 1, 5});
		rJim15_1 = rJim15.getKeyTuple(1);
		rJim57 = new QpTuple<Null>(r, new Object[] {"Jim", 5, 7});
		rJim57_1 = rJim57.getKeyTuple(1);
		
		set = new SerializedTupleSet(r);
	}
		
	@Test
	public void testAdd() {
		assertEquals(0, set.size());
		assertTrue(set.add(rNick11_1));
		assertEquals(1, set.size());
		assertFalse(set.add(rNick13_2));
		assertEquals(1, set.size());
		assertTrue(set.add(rJim23_1));
		assertEquals(2, set.size());
		assertTrue(set.add(rNick22_2));
		assertEquals(3, set.size());
		assertTrue(set.containsAtSuppliedEpoch(rNick13_2));
		assertFalse(set.containsAtSuppliedEpoch(rNick11_1));
		assertTrue(set.containsAtSuppliedEpoch(rJim23_1));
		assertTrue(set.containsAtSuppliedEpoch(rNick22_2));
	}
	
	@Test
	public void testReplace() {
		assertTrue(set.add(rNick35_1));
		assertTrue(set.add(rNick11_1));
		assertTrue(set.add(rNick24_1));
		assertFalse(set.add(rNick13_2));

		assertTrue(set.containsAtAnyEpoch(rNick11_1));
		assertTrue(set.containsAtAnyEpoch(rNick13_2));
		assertTrue(set.containsAtAnyEpoch(rNick35_1));
		assertTrue(set.containsAtAnyEpoch(rNick24_1));

		assertTrue(set.containsAtSuppliedEpoch(rNick13_2));
		assertFalse(set.containsAtSuppliedEpoch(rNick11_1));
		assertTrue(set.containsAtSuppliedEpoch(rNick35_1));
		assertTrue(set.containsAtSuppliedEpoch(rNick24_1));
		
		assertEquals(3, set.size());
	}
	
	@Test
	public void testRemove() {
		set.add(rJim15_1);
		set.add(rJim57_1);
		set.add(rJim23_1);
		assertTrue(set.removeKey(rJim23_1));
		assertFalse(set.removeKey(rJim23_1));
		assertFalse(set.containsAtAnyEpoch(rJim23_1));
		assertEquals(2, set.size());
		
		set.add(rNick11_1);
		set.add(rNick22_2);
		assertEquals(4, set.size());
		assertTrue(set.removeKey(rNick13_2));
		assertTrue(set.containsAtAnyEpoch(rNick22_2));
		assertFalse(set.containsAtAnyEpoch(rNick13_2));
		assertEquals(3, set.size());
	}
	
	@Test
	public void testIterator() {
		set.add(rJim23_1);
		set.add(rNick11_1);
		set.add(rNick22_2);
		set.add(rNick13_2);
		assertEquals(3, set.size());
		
		Set<QpTupleKey> nickTuples = new HashSet<QpTupleKey>();
		nickTuples.add(rNick22_2);
		nickTuples.add(rNick13_2);
		
		Set<QpTupleKey> jimTuples = new HashSet<QpTupleKey>();
		jimTuples.add(rJim23_1);
		
		
		int count = 0;
		for (SerializedTupleSet.SerializedKeyTuples st : set) {
			++count;
			Set<QpTupleKey> currTuples = new HashSet<QpTupleKey>();
			currTuples.addAll(st.tuples);
			if (st.tuples.size() == 1) {
				assertEquals(rJim23_1.getQPid(), st.id);
				assertEquals(jimTuples, currTuples);
			} else if (st.tuples.size() == 2) {
				assertEquals(rNick13_2.getQPid(), st.id);
				assertEquals(nickTuples, currTuples);
			} else {
				fail("Don't expect SerializedTuples with size other than 1 or 2");
			}
		}
		assertEquals(2, count);
	}
	
	public void testClear() {
		assertEquals(0, set.size());
		assertTrue(set.add(rNick11_1));
		set.clear();
		assertEquals(0, set.size());
		assertFalse(set.containsAtAnyEpoch(rNick11_1));
		assertFalse(set.containsAtSuppliedEpoch(rNick11_1));
	}

}
