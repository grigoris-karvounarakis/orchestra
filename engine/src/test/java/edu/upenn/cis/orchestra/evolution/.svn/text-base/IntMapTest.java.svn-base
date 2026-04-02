package edu.upenn.cis.orchestra.evolution;

import junit.framework.Assert;
import junit.framework.TestCase;

public class IntMapTest extends TestCase {

	public void test() {
		for (int size = 0; size < 3; size++) {
		IntMap map = new IntMap(size);
			for (int i = 0; i < 100; i++) {
				map.put(i, 42);
			}
			Assert.assertEquals(map.get(101), -1);
			Assert.assertEquals(map.get(21), 42);
		}
	}
}
