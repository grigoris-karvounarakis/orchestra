package edu.upenn.cis.orchestra.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class TestIterators {

	@Test
	public void testSubsetIterator() {
		Set<Integer> empty = Collections.emptySet();
		Iterator<Set<Integer>> it = new SubsetIterator<Integer>(empty, false);
		assertTrue("Empty set should have one subset", it.hasNext());
		assertEquals("Empty set should have empty subset", empty, it.next());
		assertFalse("Empty set should have one subset", it.hasNext());
		
		it = new SubsetIterator<Integer>(empty, true);
		assertTrue("Empty set should have one subset", it.hasNext());
		assertEquals("Empty set should have empty subset", empty, it.next());
		assertFalse("Empty set should have one subset", it.hasNext());

		Set<Integer> test = new HashSet<Integer>(Arrays.asList(1,2,3));
		Set<Set<Integer>> subsets = new HashSet<Set<Integer>>();
		subsets.add(new HashSet<Integer>());
		subsets.add(new HashSet<Integer>(Arrays.asList(1)));
		subsets.add(new HashSet<Integer>(Arrays.asList(2)));
		subsets.add(new HashSet<Integer>(Arrays.asList(3)));
		subsets.add(new HashSet<Integer>(Arrays.asList(1,2)));
		subsets.add(new HashSet<Integer>(Arrays.asList(1,3)));
		subsets.add(new HashSet<Integer>(Arrays.asList(2,3)));
		subsets.add(new HashSet<Integer>(Arrays.asList(1,2,3)));
		
		Set<Set<Integer>> results = new HashSet<Set<Integer>>();
		it = new SubsetIterator<Integer>(test,true);
		while (it.hasNext()) {
			Set<Integer> set = it.next();
			assertFalse("Duplicate value " + set, results.contains(set));
			results.add(set);
		}
		assertEquals("Incorrect result", subsets, results);
		
		Set<Set<Integer>> remaining = new HashSet<Set<Integer>>(subsets);
		it = new SubsetIterator<Integer>(test,false);
		while (it.hasNext()) {
			Set<Integer> set = it.next();
			assertTrue("Extra value " + set, remaining.remove(set));
			Set<Integer> others = new HashSet<Integer>(test);
			others.removeAll(set);
			assertTrue("Extra value " + others, remaining.remove(others));
		}
		
		assertTrue("Missing value(s) " + remaining, remaining.isEmpty());
	}
	
	@Test
	public void testCombinedIterator() {
		List<Integer> l1 = Arrays.asList(), l2 = Arrays.asList(1,2), l3 = Arrays.asList(), l3b = Arrays.asList(),
			l4 = Arrays.asList(3,4,5), l5 = Arrays.asList();
		List<Iterator<Integer>> its = new ArrayList<Iterator<Integer>>();
		its.add(l1.iterator());
		its.add(l2.iterator());
		its.add(l3.iterator());
		its.add(l3b.iterator());
		its.add(l4.iterator());
		its.add(l5.iterator());
		List<Integer> results = new ArrayList<Integer>();
		Iterator<Integer> it = new CombinedIterator<Integer>(its);
		while (it.hasNext()) {
			results.add(it.next());
		}
		assertEquals("Incorrect results", Arrays.asList(1, 2, 3, 4, 5), results);
	}
}
