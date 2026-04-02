package edu.upenn.cis.orchestra.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.IntType;
import static org.junit.Assert.*;

public class TestByteArraySet {
	byte[] one = IntType.getBytes(1), two = IntType.getBytes(2), three = IntType.getBytes(3);
	
	ByteArraySet.Deserializer<Integer> d = new ByteArraySet.Deserializer<Integer>() {

		@Override
		public Integer fromBytes(byte[] data, int offset, int length) {
			return IntType.getValFromBytes(data);
		}
		
	};
	
	@Test
	public void testEmpty() {
		ByteArraySet bas = new ByteArraySet(10);
		assertEquals(0, bas.size());
	}

	@Test
	public void testAdd() {
		ByteArraySet bas = new ByteArraySet(10);
		assertTrue(bas.add(one));
		assertFalse(bas.add(one));
		assertTrue(bas.add(two));
		assertFalse(bas.add(two));
		assertEquals(2,bas.size());
		bas.clear();
		assertFalse(bas.remove(one));
		assertFalse(bas.remove(two));
		assertEquals(0,bas.size());
	}
	
	@Test
	public void testRemove() {
		ByteArraySet bas = new ByteArraySet(10);
		assertTrue(bas.add(one));
		assertTrue(bas.add(two));
		assertTrue(bas.remove(one));
		assertEquals(1, bas.size());
		assertTrue(bas.remove(two));
		assertFalse(bas.remove(two));
		assertFalse(bas.remove(one));
		assertEquals(0,bas.size());
	}
	
	@Test
	public void testCollisions() {
		ByteArraySet bas = new ByteArraySet(10);
		int numBuckets = bas.numBuckets();
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertTrue(bas.add(IntType.getBytes(i)));
		}
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertFalse(bas.add(IntType.getBytes(i)));
		}
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertTrue(bas.remove(IntType.getBytes(i)));
		}
		for (int i = 0; i < 5 * numBuckets; ++i) {
			assertFalse(bas.remove(IntType.getBytes(i)));
		}
	}
	
	@Test
	public void testIterator() {
		Set<Integer> expected = new HashSet<Integer>();
		ByteArraySet bas = new ByteArraySet(10);
		int numBuckets = bas.numBuckets();
		int max = 5 * numBuckets;
		for (int i = 0; i < max; ++i) {
			expected.add(i);
		}
		for (int i = 0; i < max; ++i) {
			assertTrue(bas.add(IntType.getBytes(i)));
		}
		Set<Integer> found = new HashSet<Integer>();
		Iterator<Integer> it = bas.iterator(d);
		while (it.hasNext()) {
			found.add(it.next());
		}
		assertEquals(expected, found);
	}
}
