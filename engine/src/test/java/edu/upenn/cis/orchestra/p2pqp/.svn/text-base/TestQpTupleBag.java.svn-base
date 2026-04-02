package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.data.TPCH;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.JoinComparator;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.JoinFieldSource;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.RelationMapping;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.FieldSource;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.LabeledNull;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class TestQpTupleBag {
	QpSchema r, s;
	RelationMapping r2s;
	QpTuple<Null> r1, r1Nick, r1null, rnull;
	QpSchema.Source schemas;
	InetSocketAddress[] contributingNodes, contributingNodes2;
	QpTupleBag<Null> fullBag;
	final int epoch = 150;

	@Before
	public void setUp() throws Exception {
		r = new QpSchema("R", 1);
		r.addCol("a", IntType.INT);
		r.addCol("b", new StringType(true, true, true, 10));
		r.setPrimaryKey(new PrimaryKey("pk", r, Collections.singleton("a")));
		r.setCentralized();
		r.markFinished();
		schemas = new QpSchema.SingleSource(r);
		contributingNodes = new InetSocketAddress[] {new InetSocketAddress(InetAddress.getByName("www.seas.upenn.edu"), 1000),
				new InetSocketAddress(InetAddress.getByName("www.cis.upenn.edu"), 1000)};

		contributingNodes2 = new InetSocketAddress[] {new InetSocketAddress(InetAddress.getByName("www.seas.upenn.edu"), 1000),
				new InetSocketAddress(InetAddress.getByName("www.cis.upenn.edu"), 1001)};

		s = new QpSchema("S", 2);
		s.addCol("b", new StringType(true, true, true, 10));
		s.setCentralized();
		s.markFinished();

		r2s = new RelationMapping(r, s, null, new AbstractRelation.FieldSource[] { new AbstractRelation.FieldSource(1, true)});

		r = new QpSchema("R", 1);
		r.addCol("a", IntType.INT);
		r.addCol("b", new StringType(true, true, true, 10));
		r.setPrimaryKey(new PrimaryKey("pk", r, Collections.singleton("a")));
		r.setCentralized();
		r.markFinished();

		Object[] fields = new Object[2];
		fields[0] = 1;
		fields[1] = null;
		r1 = new QpTuple<Null>(r, contributingNodes, fields, 3);

		fields[0] = 1;
		fields[1] = "Nick";
		r1Nick = new QpTuple<Null>(r, contributingNodes, fields, 4);


		fields[0] = 1;
		fields[1] = new LabeledNull(99);
		r1null = new QpTuple<Null>(r, contributingNodes, fields, 5);
		fields[0] = new LabeledNull(76);
		fields[1] = "Nick";
		rnull = new QpTuple<Null>(r, contributingNodes, fields, 6);

		fullBag = new QpTupleBag<Null>(r, schemas, null, 8);
	}


	@Test
	public void testAddAndIterate() {
		fullBag.add(r1);
		fullBag.add(r1Nick);
		fullBag.add(r1null);
		fullBag.add(rnull);

		Set<QpTuple<Null>> expected = new HashSet<QpTuple<Null>>();
		expected.add(r1);
		expected.add(r1Nick);
		expected.add(r1null);
		expected.add(rnull);

		Set<QpTuple<Null>> found = new HashSet<QpTuple<Null>>();
		for (QpTuple<Null> t : fullBag) {
			assertTrue("Incorrect contributing nodes", TestQpTuple.sameContributingNodes(r1, t));
			found.add(t);
		}

		assertEquals("Incorrect results from iterating over bag", expected, found);
	}

	@Test
	public void testCreateFromSerialized() {
		fullBag.add(r1);
		fullBag.add(rnull);
		ScratchOutputBuffer out = new ScratchOutputBuffer();
		fullBag.serialize(out);

		ScratchInputBuffer in = out.getInputBuffer();
		QpTupleBag<Null> deserialized = new QpTupleBag<Null>(r, schemas, null, in);
		Iterator<QpTuple<Null>> it = deserialized.iterator();
		assertTrue(it.hasNext());
		assertEquals(r1, it.next());
		assertTrue(it.hasNext());
		assertEquals(rnull, it.next());
		assertFalse(it.hasNext());

		deserialized.add(r1Nick);
		it = deserialized.iterator();
		assertTrue(it.hasNext());
		assertEquals(r1, it.next());
		assertTrue(it.hasNext());
		assertEquals(rnull, it.next());
		assertTrue(it.hasNext());
		assertEquals(r1Nick, it.next());
		assertFalse(it.hasNext());
	}

	@Test
	public void testProjection() {
		fullBag.add(r1);
		fullBag.add(r1Nick);
		fullBag.add(r1null);
		fullBag.add(rnull);		

		QpTupleBag<Null> projected = fullBag.applyMapping(r2s, s);
		Iterator<QpTuple<Null>> it = projected.iterator();
		assertTrue(it.hasNext());
		assertEquals(new QpTuple<Null>(s, r1, r2s), it.next());
		assertTrue(it.hasNext());
		assertEquals(new QpTuple<Null>(s, r1Nick, r2s), it.next());
		assertTrue(it.hasNext());
		assertEquals(new QpTuple<Null>(s, r1null, r2s), it.next());
		assertTrue(it.hasNext());
		assertEquals(new QpTuple<Null>(s, rnull, r2s), it.next());
		assertFalse(it.hasNext());
	}

	@Test
	public void testJoin() throws Exception {
		QpTuple<Null> rTuple1 = new QpTuple<Null>(r, contributingNodes2, new Object[] {17, "Steve"}, 9);

		fullBag.add(r1Nick);
		fullBag.add(rTuple1);


		QpTupleBag<Null> sBag = new QpTupleBag<Null>(s, schemas, null);
		QpTuple<Null> sTuple1 = new QpTuple<Null>(s, contributingNodes2, new Object[] {"Nick"}, 10);
		QpTuple<Null> sTuple2 = new QpTuple<Null>(s, contributingNodes2, new Object[] {"Jim"}, 11);
		sBag.add(sTuple1);
		sBag.add(sTuple2);

		JoinComparator jc = new JoinComparator(r, s, new int[] {1}, new int[] {0});
		RelationMapping rm = new RelationMapping(r, s, s, new JoinFieldSource[] {new JoinFieldSource(0, false)});

		QpTupleBag<Null> outBag = new QpTupleBag<Null>(s, schemas, null);

		Iterator<ByteArrayWrapper>  it = fullBag.serializedIterator();
		while (it.hasNext()) {
			ByteArrayWrapper baw = it.next();
			sBag.joinWith(15, r, baw.array, baw.offset, outBag, rm, jc);
		}
		List<QpTuple<Null>> outTuples = new ArrayList<QpTuple<Null>>();
		for (QpTuple<Null> t : outBag) {
			outTuples.add(t);
		}
		assertEquals("Wrong number of output tuples from join", 1, outTuples.size());
		QpTuple<Null> t = outTuples.get(0);
		assertEquals("Wrong output tuple from join", sTuple1, t);
		assertEquals("Wrong number of contributing nodes from joined tuple", 3, t.getNumContributingNodes());
		Set<InetSocketAddress> expectedContributingNodes = new HashSet<InetSocketAddress>();
		for (InetSocketAddress isa : contributingNodes) {
			expectedContributingNodes.add(isa);
		}
		for (InetSocketAddress isa : contributingNodes2) {
			expectedContributingNodes.add(isa);
		}
		assertEquals("Incorrect contributing nodes in joined tuple", expectedContributingNodes, new HashSet<InetSocketAddress>(Arrays.asList(t.getContributingNodes())));
	}

	@Test
	public void testAddContributingNodes() throws Exception {
		QpTupleBag<Null> sBag = new QpTupleBag<Null>(s, schemas, null);
		QpTuple<Null> sTuple1 = new QpTuple<Null>(s, contributingNodes, new Object[] {"Nick"}, 10);
		QpTuple<Null> sTuple2 = new QpTuple<Null>(s, contributingNodes, new Object[] {"Jim"}, 11);
		sBag.add(sTuple1);
		sBag.add(sTuple2);

		Set<InetSocketAddress> expected = new HashSet<InetSocketAddress>(Arrays.asList(contributingNodes[0], contributingNodes[1], contributingNodes2[1]));

		byte[] already = OutputBuffer.getBytes(contributingNodes2[0]);
		byte[] toAdd = OutputBuffer.getBytes(contributingNodes2[1]);

		QpTupleBag<Null> added = sBag.addContributingNode(toAdd);
		added = added.addContributingNode(already);

		for (QpTuple<Null> t : added) {
			InetSocketAddress[] nodes = t.getContributingNodes();
			assertEquals("Wrong number of contributing nodes", 3, nodes.length);
			assertEquals("Wrong contributing nodes", expected, new HashSet<InetSocketAddress>(Arrays.asList(nodes)));
		}	
	}

	@Test
	public void testDate() throws Exception {
		QpSchema t = new QpSchema("T", 5);
		t.addCol("a", new IntType(false, false));
		t.addCol("b", new DateType(false, false));
		t.markFinished();

		QpTuple<Null> t1 = new QpTuple<Null>(t, new Object[] {1, new Date(1999, 11, 1)});
		QpTuple<Null> t2 = new QpTuple<Null>(t, new Object[] {2, new Date(2000, 1, 31)});
		
		QpTupleBag<Null> ts = new QpTupleBag<Null>(t, null, null);
		ts.add(t1);
		ts.add(t2);
		
		List<QpTuple<Null>> expected = new ArrayList<QpTuple<Null>>();
		expected.add(t1);
		expected.add(t2);
		List<QpTuple<Null>> list = new ArrayList<QpTuple<Null>>();
		for (QpTuple<Null> tuple : ts) {
			list.add(tuple);
		}
		assertEquals(expected, list);
	}
	
	@Test
	public void testIdentityMapping() throws Exception {
		QpSchema LINEITEM = TPCH.simpleQpSchemas.get("LINEITEM");
		QpTuple<Null> l1 = new QpTuple<Null>(LINEITEM, new Object[] { 1,2,3,4,5.0,6.0,7.0,8.0,"Y","N",new Date(1990,1,1), new Date(1991,1,1),
				new Date(1993,1,1), "Hello", "Goodbye", "Comment1"});
		QpTupleBag<Null> ls = new QpTupleBag<Null>(LINEITEM, null, null);
		ls.add(l1);
		ls.add(l1);
		ls.add(l1);
		ls.add(l1);
		ls.add(l1);
		
		int count = 0;
		for (QpTuple<Null> t : ls) {
			assertEquals("Incorrect tuple from bag", l1, t);
			++count;
		}
		assertEquals("Incorrect number of tuples from bag", 5, count);
		
		QpSchema copy = new QpSchema("LINEITEMC", 99);
		for (int i = 0; i < LINEITEM.getNumCols(); ++i) {
			copy.addCol(LINEITEM.getColName(i), LINEITEM.getColType(i));
		}
		copy.markFinished();
		RelationMapping rm = new RelationMapping(LINEITEM, copy, null, new FieldSource[] {
				new FieldSource(0, true),
				new FieldSource(1, true),
				new FieldSource(2, true),
				new FieldSource(3, true),
				new FieldSource(4, true),
				new FieldSource(5, true),
				new FieldSource(6, true),
				new FieldSource(7, true),
				new FieldSource(8, true),
				new FieldSource(9, true),
				new FieldSource(10, true),
				new FieldSource(11, true),
				new FieldSource(12, true),
				new FieldSource(13, true),
				new FieldSource(14, true),
				new FieldSource(15, true),
		});
		
		QpTuple<Null> copyTuple = new QpTuple<Null>(copy, l1, rm, (InetSocketAddress[]) null, (Null) null, (MetadataFactory<Null>) null, 0);

		QpTupleBag<Null> copyBag = ls.applyMapping(rm, copy);
		count = 0;
		for (QpTuple<Null> t : copyBag) {
			assertEquals("Incorrect tuple from copy bag", copyTuple, t);
			++count;
		}
		assertEquals("Incorrect number of tuples from copy bag", 5, count);
	}
}
