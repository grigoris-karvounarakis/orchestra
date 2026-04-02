package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.DoubleType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.FieldSource;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.RelationMapping;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple.LabeledNull;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class TestQpTuple {

	QpSchema r;
	QpSchema s;
	QpTuple<Null> r1, r1Nick, r1null, rnull;
	QpSchema.Source schemas;

	InetSocketAddress[] contributingNodes;

	@Before
	public void createSchema() throws Exception {
		contributingNodes = new InetSocketAddress[] {new InetSocketAddress(InetAddress.getByName("www.seas.upenn.edu"), 1000),
				new InetSocketAddress(InetAddress.getByName("www.cis.upenn.edu"), 1000)};

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
		
		s = new QpSchema("S", 2);
		s.addCol("a", new IntType(false,false));
		s.addCol("b", new StringType(false, false, true, 10));
		s.setCentralized();
		s.markFinished();

		schemas = new QpSchema.CollectionSource(r, s);
	}

	@Test
	public void testKeySerialization() throws Exception {
		QpTupleKey key = r1.getKeyTuple(95);
		byte[] keyBytes = key.getBytes();
		QpTupleKey t = new QpTupleKey(r, keyBytes, 0, keyBytes.length);
		
		assertEquals("Incorrect key deserialization", key, t);
		assertEquals("Incorrect tuple epoch", key.epoch, t.epoch);

		assertEquals("Incorrect key generation", r1Nick.getKeyTuple(95), t);
	}

	@Test
	public void testStoreSerialization() throws Exception {
		byte[] storeBytes = r1Nick.getStoreBytes();

		
		QpTuple<?> t = QpTuple.fromStoreBytes(r, storeBytes, null, 99);
		assertEquals("Incorrect store deserialization", r1Nick, t);
		assertFalse("Should not have contributing nodes in store tuple", t.hasContributingNodes());

		storeBytes = r1null.getStoreBytes();
		t = QpTuple.fromStoreBytes(r, storeBytes, null, 99);
		assertEquals("Incorrect store deserialization", r1null, t);
		assertFalse("Should not have contributing nodes in store tuple", t.hasContributingNodes());
	}

	@Test
	public void testExecutionSerialization() throws Exception {
		ScratchOutputBuffer sob = new ScratchOutputBuffer();
		r1Nick.getBytes(sob);
		ScratchInputBuffer sib = sob.getInputBuffer();
		QpTuple<?> t = QpTuple.fromBytes(r, sib);
		assertEquals("Incorrect execution deserialization", r1Nick, t);
		assertEquals("Incorrect tuple phase", r1Nick.getPhase(), t.getPhase());
		assertTrue("Incorrect contributing nodes", sameContributingNodes(r1Nick, t));
		assertTrue("Incorrect contributing nodes", r1Nick.sameContributingNodes(t));
	}

	@Test(expected=ValueMismatchException.class)
	public void testTypeError() throws Exception {
		new QpTuple<Null>(r, new Object[] {"Fred", null});
	}

	@Test
	public void testFullTuple() {
		assertEquals("Incorrect key value", 1, r1Nick.get(0));
		assertEquals("Incorrect non-key value", "Nick", r1Nick.get(1));
	}

	@Test
	public void testCreateKeyTuple() {
		QpTupleKey key = r1Nick.getKeyTuple(99);
		assertTrue(key.isKeyTuple());
		assertEquals(99, key.epoch);
		assertEquals(r1Nick.get(0), key.get(0));
		assertNull(key.get(1));
		assertEquals(r1, key);
	}

	@Test
	public void testCreateNonKeyTuple() {
		QpTupleKey key = r1Nick.getKeyTuple(100);
		QpTuple<?> nonKey = key.getNonKeyTuple(97, null, null);
		assertEquals(1,nonKey.get(0));
		assertTrue(nonKey.isNull(1));
		assertFalse(nonKey.isLabeledNull(1));

		key = rnull.getKeyTuple(100);
		nonKey = key.getNonKeyTuple(97, null, null);
		assertTrue(nonKey.isNull(0));
		assertTrue(nonKey.isNull(1));
		assertTrue(nonKey.isLabeledNull(0));
		assertEquals(rnull.getLabeledNull(0), nonKey.getLabeledNull(0));
	}

	@Test
	public void testLabeledNull() throws Exception {
		assertTrue(r1null.isLabeledNull(1));
		assertTrue(r1null.isNull(1));
		assertEquals(99, r1null.getLabeledNull(1));
		assertNull(r1null.get(1));
		assertEquals(new LabeledNull(99), r1null.getValueOrLabeledNull(1));
	}

	@Test
	public void testNull() throws Exception {
		assertTrue(r1.isNull(1));
		assertFalse(r1.isLabeledNull(1));
		assertNull(r1.get(1));
	}
	
	@Test
	public void testLabeledNullKey() {
		assertEquals(76, rnull.getKeyTuple(5).getLabeledNull(0));
		assertEquals(rnull.getKeyTuple(5), rnull.getKeyTuple(5));
	}

	@Test
	public void testEquals() throws Exception {
		QpTupleKey key = r1Nick.getKeyTuple(17);
		QpTuple<?> r1n = new QpTuple<Null>(r, new Object[] {1, null});
		assertEquals(r1n, key);
	}

	@Test
	public void testHashcode() throws Exception {
		QpMutableTuple<?> r1NickMT = new QpMutableTuple<Null>(r1Nick, null);
		assertEquals(r1NickMT.hashCode(), r1Nick.hashCode());
	}

	static boolean sameContributingNodes(QpTuple<?> t1, QpTuple<?> t2) {
		InetSocketAddress[] cn1array = t1.getContributingNodes(), cn2array = t2.getContributingNodes();
		HashSet<InetSocketAddress> cn1 = new HashSet<InetSocketAddress>(Arrays.asList(cn1array));
		HashSet<InetSocketAddress> cn2 = new HashSet<InetSocketAddress>(Arrays.asList(cn2array));
		return cn1.equals(cn2);
	}
	
	@Test(expected=ValueMismatchException.class)
	public void testSetNonNullableNull() throws Exception {
		new QpTuple<Null>(s, new Object[] {null, "hi!"});
	}

	@Test(expected=ValueMismatchException.class)
	public void testSetNonNullableLabeledNull() throws Exception {
		new QpTuple<Null>(s, new Object[] {new LabeledNull(4), "hi!"});
	}
	
	@Test(expected=ValueMismatchException.class)
	public void testInvalidRetainRelationMapping() throws ValueMismatchException {
		new RelationMapping(s, new int[] {1});		
	}

	@Test(expected=ValueMismatchException.class)
	public void testInvalidRelationMapping() throws ValueMismatchException {
		new RelationMapping(r, s, null, new FieldSource[] {new FieldSource(0,true), new FieldSource(1,true)});		
	}

	@Test
	public void testValidRelationMapping() throws ValueMismatchException {
		new RelationMapping(s, r, null, new FieldSource[] {new FieldSource(0,true), new FieldSource(1,true)});		
	}
	
	@Test
	public void testEquality() throws Exception {
		QpSchema t = new QpSchema("T", 100);
		t.addCol("a", new IntType(false, false));
		t.addCol("b", new DoubleType(false, false));
		t.markFinished();
		
		QpTuple<Null> t1 = new QpTuple<Null>(t, new Object[] {5, 100.000001});
		QpTuple<Null> t2 = new QpTuple<Null>(t, new Object[] {5, 100.000002});
		QpTuple<Null> t3 = new QpTuple<Null>(t, new Object[] {5, 100.02});
		
		assertTrue("Close doubles should be equal", t1.equals(t2));
		assertFalse("Far apart doubles should not be equal", t1.equals(t3));
	}
	
	@Test(expected=IllegalArgumentException.class)
	public void testCreateNonKeyTupleException() throws Exception {
		QpSchema t = new QpSchema("T", 101);
		t.addCol("a", new IntType(false, false));
		t.addCol("b", new IntType(false, false));
		t.setPrimaryKey(Arrays.asList("a"));
		t.markFinished();

		QpTuple<Null> t1 = new QpTuple<Null>(t, new Object[] {1, 2});
		QpTupleKey t1Key = t1.getKeyTuple(77);
		t1Key.getNonKeyTuple(0, null, null);
	}	
}