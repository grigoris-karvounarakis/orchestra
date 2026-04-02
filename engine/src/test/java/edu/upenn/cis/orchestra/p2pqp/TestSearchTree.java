package edu.upenn.cis.orchestra.p2pqp;

import static edu.upenn.cis.orchestra.p2pqp.Id.ZERO;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.IntType;

public class TestSearchTree {
	QpSchema r;
	Id id1, id5, id10, id15, id20, id25, id30, id35, id40, id45, id50;
	Id id23, id47, id100;
	List<Id> ids;
	List<IdRange> ranges;
	
	
	@Before
	public void setUp() throws Exception {
		r = new QpSchema("R", 1);
		r.addCol("a", IntType.INT);
		r.markFinished();
		
		byte[] bytes = new byte[Id.idLengthBytes];
		
		bytes[0] = 1;
		id1 = Id.fromMSBBytes(bytes);
		bytes[0] = 5;
		id5 = Id.fromMSBBytes(bytes);
		bytes[0] = 10;
		id10 = Id.fromMSBBytes(bytes);
		bytes[0] = 15;
		id15 = Id.fromMSBBytes(bytes);
		bytes[0] = 20;
		id20 = Id.fromMSBBytes(bytes);
		bytes[0] = 25;
		id25 = Id.fromMSBBytes(bytes);
		bytes[0] = 30;
		id30 = Id.fromMSBBytes(bytes);
		bytes[0] = 35;
		id35 = Id.fromMSBBytes(bytes);
		bytes[0] = 40;
		id40 = Id.fromMSBBytes(bytes);
		bytes[0] = 45;
		id45 = Id.fromMSBBytes(bytes);
		bytes[0] = 50;
		id50 = Id.fromMSBBytes(bytes);

		ids = new ArrayList<Id>();
		ranges = new ArrayList<IdRange>();
		ids.add(id1);
		ids.add(id5);
		ids.add(id10);
		ids.add(id15);
		ids.add(id20);
		ids.add(id25);
		ids.add(id30);
		ids.add(id35);
		ids.add(id40);
		ids.add(id45);
		ids.add(id50);
		
		ranges.add(new IdRange(Id.ZERO, id5));
		ranges.add(new IdRange(id5, id10));
		ranges.add(new IdRange(id10, id15));
		ranges.add(new IdRange(id15, id20));
		ranges.add(new IdRange(id20, id25));
		ranges.add(new IdRange(id25, id30));
		ranges.add(new IdRange(id30, id35));
		ranges.add(new IdRange(id35, id40));
		ranges.add(new IdRange(id40, id45));
		ranges.add(new IdRange(id45, id50));
		ranges.add(new IdRange(id50, Id.ZERO));
		
		bytes[0] = 23;
		id23 = Id.fromMSBBytes(bytes);
		bytes[0] = 47;
		id47 = Id.fromMSBBytes(bytes);
		bytes[0] = 100;
		id100 = Id.fromMSBBytes(bytes);
	}
	
	
	@Test
	public void testSimpleSearchTree() throws Exception {
		List<Id> fewKeys = new ArrayList<Id>();
		fewKeys.add(id1);
		fewKeys.add(id5);
		
		SearchTree st = new SearchTree(3, fewKeys);
		assertEquals(0,st.getIndexPage(id1));
		assertEquals(1,st.getIndexPage(id5));
		assertEquals(0,st.getIndexPage(ZERO));
		assertEquals(1,st.getIndexPage(id47));
	}
	
	@Test
	public void testSearchTree() throws Exception {
		SearchTree st = new SearchTree(3, ids);
		assertEquals(0,st.getIndexPage(id1));
		assertEquals(1,st.getIndexPage(id5));
		
		assertEquals(9, st.getIndexPage(id47));
		assertEquals(10, st.getIndexPage(id100));
		assertEquals(0,st.getIndexPage(ZERO));
	}
	
	@Test
	public void testDeserialization() throws Exception {
		SearchTree st = new SearchTree(3, ids);
		byte[] serialized = st.getTree();
		st = new SearchTree(serialized);
		assertEquals(0,st.getIndexPage(id1));
		assertEquals(1,st.getIndexPage(id5));
		assertEquals(9, st.getIndexPage(id47));
		assertEquals(10, st.getIndexPage(id100));
		assertEquals(0,st.getIndexPage(ZERO));
	}
	
	@Test
	public void testRelevantPages() {
		SearchTree st = new SearchTree(3, ids);
		
		List<Integer> pages = st.getRelevantPages(new IdRange(ZERO,id1));
		assertEquals(Arrays.asList(0),pages);
		
		pages = st.getRelevantPages(new IdRange(id23,id47));
		assertEquals(Arrays.asList(4,5,6,7,8,9),pages);
	}
	
	@Test
	public void testPageRanges() {
		SearchTree st = new SearchTree(3, ids);
		List<IdRange> ranges = st.getPageRanges();
		assertEquals(this.ranges, ranges);
	}
}
