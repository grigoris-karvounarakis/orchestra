package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;


public class TestCache implements Cache.EvictionHandler<Integer,Integer> {
	Integer one, two, three, four;
	LRUCache<Integer,Integer> cache;
	ArrayList<Integer> evictedKeys;
	ArrayList<Integer> evictedValues;

	public void wasEvicted(Integer key, Integer value) {
		evictedKeys.add(key);
		evictedValues.add(value);
	}

	@Before
	public void setUp() throws Exception {
		one = 1;
		two = 2;
		three = 3;
		four = 4;
		evictedKeys = new ArrayList<Integer>();
		evictedValues = new ArrayList<Integer>();
	}
	
	@Test
	public void testInsertionAndEviction() throws Exception {
		cache = new EntryBoundLRUCache<Integer,Integer>(2, this);
		cache.store(one, one);
		assertNotNull(cache.probe(one));
		cache.store(two, two);
		assertNotNull(cache.probe(two));
		cache.store(three, three);
		assertNotNull(cache.probe(three));
		assertNull(cache.probe(one));
		assertEquals(1, evictedKeys.size());
		assertEquals(1, evictedValues.size());
		int evictedKey = evictedKeys.get(0);
		int evictedValue = evictedValues.get(0);
		assertEquals(1, evictedKey);
		assertEquals(1, evictedValue);
	}
	
	@Test
	public void testClear() throws Exception {
		cache = new EntryBoundLRUCache<Integer,Integer>(2, this);
		cache.store(one, one);
		assertNotNull(cache.probe(one));
		cache.clear(one);
		assertNull(cache.probe(one));
	}

}
