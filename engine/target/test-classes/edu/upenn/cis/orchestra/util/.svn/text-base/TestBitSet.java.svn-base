package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class TestBitSet {
	@Test
	public void testCreate() {
		BitSet bs = new BitSet(8);
		assertEquals(1, bs.getData().length);
		bs = new BitSet(9);
		assertEquals(2, bs.getData().length);
		bs = new BitSet(15);
		assertEquals(2, bs.getData().length);
	}
	
	@Test
	public void testAllZeros() {
		BitSet bs = new BitSet(15);
		for (int i = 0; i < 15; ++i) {
			assertFalse(bs.get(i));
		}
	}
	
	@Test
	public void testAllOnes() {
		BitSet bs = new BitSet(15);
		for (int i = 0; i < 15; ++i) {
			bs.set(i);
		}		
		for (int i = 0; i < 15; ++i) {
			assertTrue(bs.get(i));
		}
	}
	
	@Test
	public void testSome() {
		BitSet bs = new BitSet(32);
		Set<Integer> set = new HashSet<Integer>();
		int[] vals = {3, 7, 8, 19, 25, 31};
		for (int val : vals) {
			set.add(val);
			bs.set(val);
		}
		Set<Integer> found = new HashSet<Integer>();
		for (int i = 0; i < 32; ++i) {
			if (bs.get(i)) {
				found.add(i);
			}
		}
		assertEquals(set,found);
	}
}
