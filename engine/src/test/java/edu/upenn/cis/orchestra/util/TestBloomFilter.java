package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.*;

import org.junit.Test;


public class TestBloomFilter {
	private final BloomFilter.Hasher<Integer> hasher = new BloomFilter.Hasher<Integer>() {
		@Override
		public int hashCode1(Integer val) {
			return val;
		}

		@Override
		public int hashCode2(Integer val) {
			return 37 * val + 1;
		}
	};
	
	@Test
	public void testInsertion() {
		BloomFilter<Integer> bf = new BloomFilter<Integer>(100, 3, hasher);
		bf.add(17);
	}
	
	@Test
	public void testContains() {
		BloomFilter<Integer> bf = new BloomFilter<Integer>(100, 3, hasher);
		assertFalse(bf.contains(17));
		bf.add(17);
		assertTrue(bf.contains(17));
	}
	
	@Test
	public void testDoesNotContain() {
		BloomFilter<Integer> bf = new BloomFilter<Integer>(100, 3, hasher);
		bf.add(17);
		bf.add(91);
		bf.add(999);
		assertFalse(bf.contains(19));
	}
}
