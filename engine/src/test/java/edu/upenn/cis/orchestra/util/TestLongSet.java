package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.*;

import org.junit.Test;

public class TestLongSet {
	@Test
	public void testAdd() {
		LongSet ls = new LongSet(3);
		assertTrue(ls.add(1));
		assertTrue(ls.add(2));
		assertTrue(ls.add(4));
		assertFalse(ls.add(4));
		
		assertTrue(ls.contains(1));
		assertTrue(ls.contains(2));
		assertTrue(ls.contains(1));
		assertEquals(3, ls.size());
	}
	
	@Test
	public void testRemove() {
		LongSet ls = new LongSet(3);
		assertTrue(ls.add(1));
		assertTrue(ls.add(4));
		assertTrue(ls.remove(1));
		assertTrue(ls.contains(4));
		assertEquals(1,ls.size());
		assertTrue(ls.add(1));
		assertTrue(ls.contains(1));
		assertTrue(ls.contains(4));
	}
	
	@Test
	public void testRemoveFrom() {
		LongSet ls = new LongSet(17);
		assertTrue(ls.add(1));
		assertTrue(ls.add(4));
		assertTrue(ls.add(15));
		assertTrue(ls.add(18));
		assertTrue(ls.add(92));

		LongSet ls2 = new LongSet(19);
		assertTrue(ls2.add(1));
		assertTrue(ls2.add(20));
		assertTrue(ls2.add(18));
		
		assertFalse(ls.equals(ls2));
		
		ls.removeAll(ls2);
		assertEquals(3,ls.size());
		assertTrue(ls.contains(4));
		assertTrue(ls.contains(15));
		assertTrue(ls.contains(92));
	}
}
