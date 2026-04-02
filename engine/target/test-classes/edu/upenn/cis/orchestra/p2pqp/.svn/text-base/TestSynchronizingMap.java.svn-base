package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation.BadColumnName;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;


public class TestSynchronizingMap {
	
	SynchronizingMap.SupportingOps<Integer,Integer> vf;
	
	@Before
	public void setUp() throws BadColumnName, ValueMismatchException {
		vf = new SynchronizingMap.SupportingOps<Integer,Integer>() {
			int next = 0;
			public synchronized Integer getNewValue() {
				return next++;
			}
			@Override
			public boolean equals(Integer key1, Integer key2) {
				return key1 == key2;
			}
			@Override
			public int hash(Integer key) {
				return key;
			}
		};
	}

	@Test
	public void testInsertTwice() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf);
		int v1 = map.getOrCreate(1);
		int v2 = map.getOrCreate(3);
		assertEquals(2, map.size());
		
		assertTrue(v1 != v2);
		assertEquals(v1, map.getOrCreate(1));
		assertEquals(v2, map.getOrCreate(3));
	}
	
	@Test
	public void testCollision() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 1, Float.MAX_VALUE);
		int v1 = map.getOrCreate(1);
		int v2 = map.getOrCreate(3);
		assertEquals(2, map.size());
		
		assertTrue(v1 != v2);
		assertEquals(v1, map.getOrCreate(1));
		assertEquals(v2, map.getOrCreate(3));		
	}
	
	@Test
	public void testResize() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 1, 1.0f);
		int v1 = map.getOrCreate(1);
		int v2 = map.getOrCreate(3);
		assertEquals(2, map.size());
		
		assertTrue(v1 != v2);
		assertEquals(v1, map.getOrCreate(1));
		assertEquals(v2, map.getOrCreate(3));
	}
	
	@Test
	public void testInterator() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf);
		int v1 = map.getOrCreate(1);
		int v2 = map.getOrCreate(3);

		HashSet<Integer> expected = new HashSet<Integer>();
		expected.add(v1);
		expected.add(v2);
		
		HashSet<Integer> found = new HashSet<Integer>();
		for (SynchronizingMap<Integer,Integer>.Entry e : map) {
			found.add(e.value);
		}
		assertEquals(expected,found);
	}
	
	@Test
	public void testRemoveSameBucket() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 1, Float.MAX_VALUE);
		map.getOrCreate(1);
		int v2 = map.getOrCreate(3);
		assertEquals(2, map.size());

		assertTrue(map.remove(1));
		
		assertTrue(map.contains(3));
		assertFalse(map.contains(1));
		assertEquals(v2, map.getOrCreate(3));
		assertEquals(1, map.size());
	}
	
	@Test
	public void testRemoveDifferentBuckets() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 1, 0.75f);
		map.getOrCreate(1);
		int v2 = map.getOrCreate(3);
		assertEquals(2, map.size());

		assertTrue(map.remove(1));
		
		assertTrue(map.contains(3));
		assertFalse(map.contains(1));
		assertEquals(v2, map.getOrCreate(3));
		assertEquals(1, map.size());
	}
	
	
	@Test
	public void testTupleMapIteratorDeleteBucket() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 1, Float.MAX_VALUE);
		map.getOrCreate(1);
		map.getOrCreate(3);

		Iterator<SynchronizingMap<Integer,Integer>.Entry> it = map.iterator();
		
		while (it.hasNext()) {
			it.next();
			it.remove();
		}
		
		assertEquals(0, map.size());
		assertFalse(map.contains(1));
		assertFalse(map.contains(3));
	}

	@Test
	public void testTupleMapIteratorDeletePartialBucket() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 1, Float.MAX_VALUE);
		map.getOrCreate(1);
		map.getOrCreate(2);
		map.getOrCreate(3);

		Iterator<SynchronizingMap<Integer,Integer>.Entry> it = map.iterator();
		
		while (it.hasNext()) {
			SynchronizingMap<Integer,Integer>.Entry e = it.next();
			if (e.key == 2) {
				it.remove();				
			}
		}
		
		assertEquals(2, map.size());
		assertFalse(map.contains(2));
		assertTrue(map.contains(1));
		assertTrue(map.contains(3));
	}

	@Test
	public void testTupleMapIteratorDeleteMultipleBuckets() {
		SynchronizingMap<Integer,Integer> map = new SynchronizingMap<Integer,Integer>(vf, 30, Float.MAX_VALUE);
		map.getOrCreate(1);
		map.getOrCreate(2);
		map.getOrCreate(3);

		Iterator<SynchronizingMap<Integer,Integer>.Entry> it = map.iterator();
		
		while (it.hasNext()) {
			SynchronizingMap<Integer,Integer>.Entry e = it.next();
			int key = e.key; 
			if (key == 2 || key == 3) {
				it.remove();				
			}
		}
		
		assertEquals(1, map.size());
		assertFalse(map.contains(2));
		assertTrue(map.contains(1));
		assertFalse(map.contains(3));
	}

}
