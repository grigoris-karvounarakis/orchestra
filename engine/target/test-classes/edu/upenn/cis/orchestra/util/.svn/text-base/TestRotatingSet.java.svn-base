package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.*;

import org.junit.Test;


public class TestRotatingSet {

	@Test
	public void testAdd() {
		RotatingSet<Integer> set = new RotatingSet<Integer>();
		assertTrue(set.add(3));
		assertTrue(set.add(4));
		assertTrue(set.add(5));
		assertFalse(set.add(4));
		assertEquals(3,set.next());
		assertEquals(4,set.next());
		assertEquals(5,set.next());
		assertEquals(3,set.next());
		assertEquals(4,set.next());
		assertEquals(5,set.next());
	}
	
	@Test
	public void testRemove() {
		RotatingSet<Integer> set = new RotatingSet<Integer>();
		assertTrue(set.add(3));
		assertTrue(set.add(4));
		assertTrue(set.add(5));
		assertEquals(3,set.next());
		assertTrue(set.remove(3));
		assertFalse(set.remove(3));
		assertEquals(4,set.next());
		assertEquals(5,set.next());
		assertEquals(4,set.next());
		assertEquals(5,set.next());
		assertTrue(set.remove(4));
		assertFalse(set.remove(4));
		assertEquals(5,set.next());
		assertTrue(set.add(4));
		assertEquals(5,set.next());
		assertEquals(4,set.next());		
	}
}
