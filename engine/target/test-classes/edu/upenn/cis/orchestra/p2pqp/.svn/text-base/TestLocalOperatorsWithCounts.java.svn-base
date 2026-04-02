package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator.AggColumn;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator.OutputColumn;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;

public class TestLocalOperatorsWithCounts {
	QpSchema r, s;
	QpSchema.Source schemas;
	
	QpTuple<Integer> r111, r112, r113, r121, r131;
	QpTuple<Integer> s15, s36, s37;
	List<QpTuple<Integer>> rTuples, sTuples;
	
	final MetadataFactory<Integer> mdf = CountMetadataFactory.getInstance();
	final boolean allowRecovery = false;
	InetSocketAddress nodeId;
	final int queryId = 17;
	
	
	@Before
	public void setUp() throws Exception {
		nodeId = new InetSocketAddress(InetAddress.getLocalHost(), 5000);
		
		r = new QpSchema("R", 1);
		r.addCol("a", IntType.INT);
		r.addCol("b", IntType.INT);
		r.addCol("c", IntType.INT);
		r.setPrimaryKey(new PrimaryKey("pk", r, Arrays.asList("a", "b", "c")));
		r.markFinished();

		s = new QpSchema("S", 2);
		s.addCol("b", IntType.INT);
		s.addCol("c", IntType.INT);
		s.setPrimaryKey(new PrimaryKey("pk", s, Collections.singleton("b")));
		s.markFinished();
		
		schemas = new QpSchema.CollectionSource(Arrays.asList(r, s));
		
		r111 = new QpTuple<Integer>(r, new Object[] {1,1,1});
		r112 = new QpTuple<Integer>(r, new Object[] {1,1,2});
		r113 = new QpTuple<Integer>(r, new Object[] {1,1,3});
		r121 = new QpTuple<Integer>(r, new Object[] {1,2,1});
		r131 = new QpTuple<Integer>(r, new Object[] {1,3,1});
		
		s15 = new QpTuple<Integer>(s, new Object[] {1,5});
		s36 = new QpTuple<Integer>(s, new Object[] {3,6});
		s37 = new QpTuple<Integer>(s, new Object[] {3,7});
		
		List<QpTuple<Integer>> rTuples = new ArrayList<QpTuple<Integer>>();
		rTuples.add(r111);
		rTuples.add(r112);
		rTuples.add(r113);
		rTuples.add(r121);
		rTuples.add(r131);
		this.rTuples = Collections.unmodifiableList(rTuples);
		
		List<QpTuple<Integer>> sTuples = new ArrayList<QpTuple<Integer>>();
		sTuples.add(s15);
		sTuples.add(s36);
		sTuples.add(s37);
		this.sTuples = Collections.unmodifiableList(sTuples);
	}
	
	@Test
	public void testJoin() throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		SpoolOperator<Integer> spool = SpoolOperator.create(r, schemas, nodeId, 10, rt, mdf, allowRecovery, false);
		PipelinedHashJoin<Integer> phj = new PipelinedHashJoin<Integer>(r, s, Collections.singletonList(1), Collections.singletonList(0),
				Arrays.asList(0, 1, null), Arrays.asList(null, 2), r, spool, null, nodeId, 3, rt, mdf, schemas, null);
		CollectionScan<Integer> rScan = new CollectionScan<Integer>(r, null, rTuples, phj, WhichInput.LEFT, queryId, nodeId, 1, rt, mdf, false);
		CollectionScan<Integer> sScan = new CollectionScan<Integer>(s, null, sTuples, phj, WhichInput.RIGHT, queryId, nodeId, 2, rt, mdf, false);
		
		rScan.scan(1, 0);
		sScan.scanAll(0);
		rScan.scanAll(0);
		
		List<QpTuple<Integer>> withMetadata = spool.getResultsWithMetadata();
		Map<QpTuple<Null>,Integer> fromWithMetadata = new HashMap<QpTuple<Null>,Integer>();
		for (QpTuple<Integer> t : withMetadata) {
			fromWithMetadata.put(t.changeMetadata((Null)null, null), t.getMetadata(schemas, mdf));
		}
		List<QpTuple<Null>> withoutMetadata = spool.getResultsWithoutMetadata();
		Map<QpTuple<Null>,Integer> fromWithoutMetadata = new HashMap<QpTuple<Null>,Integer>();
		for (QpTuple<Null> t : withoutMetadata) {
			Integer already = fromWithoutMetadata.get(t);
			if (already == null) {
				fromWithoutMetadata.put(t, 1);
			} else {
				fromWithoutMetadata.put(t, already + 1);
			}
			
		}
		
		Map<QpTuple<Null>,Integer> expected = new HashMap<QpTuple<Null>,Integer>();
		
		QpTuple<Null> r115 = new QpTuple<Null>(r, new Object[] {1,1,5});
		QpTuple<Null> r136 = new QpTuple<Null>(r, new Object[] {1,3,6});
		QpTuple<Null> r137 = new QpTuple<Null>(r, new Object[] {1,3,7});
		
		expected.put(r115, 3);
		expected.put(r136, 1);
		expected.put(r137, 1);
		
		assertEquals("Incorrect results from results retrieved without counts", expected, fromWithoutMetadata);
		assertEquals("Incorrect results from results retrieved with counts", expected, fromWithMetadata);
		
		QpTuple<Integer> remove = r111.changeMetadata(-1, mdf);
		QpTupleBag<Integer> removeList = new QpTupleBag<Integer>(r, schemas, mdf);
		removeList.add(remove);
		phj.receiveTuples(WhichInput.LEFT, removeList);
		
		expected.put(r115, 2);
		withMetadata = spool.getResultsWithMetadata();
		fromWithMetadata.clear();
		for (QpTuple<Integer> t : withMetadata) {
			fromWithMetadata.put(t.changeMetadata((Null)null, null), t.getMetadata(schemas, mdf));
		}
		assertEquals("Incorrect results from results retrieved with counts after deletion", expected, fromWithMetadata);
	}
	
	@Test
	public void testAggregation() throws Exception {
		QpSchema t = new QpSchema("T",3);
		t.addCol("a", IntType.INT);
		t.addCol("sum", IntType.INT);
		t.markFinished();
		RecordTuplesTest rt = new RecordTuplesTest();
		SpoolOperator<Integer> spool = SpoolOperator.create(t, schemas, nodeId, 10, rt, mdf, allowRecovery, false);
		List<OutputColumn> ocs = Arrays.asList(new OutputColumn(0), new AggColumn(1, AggFunc.SUM));
		HashAggregator<Integer> agg = new HashAggregator<Integer>(spool, null, r, t, Collections.singletonList(0), ocs,
				nodeId, 2, rt, mdf, schemas, null, null);
		CollectionScan<Integer> rScan = new CollectionScan<Integer>(r, null, rTuples, agg, null, queryId, nodeId, 1, rt, mdf, false);
		rScan.scanAll(0);
		QpTuple<Integer> remove = r111.changeMetadata(-1, mdf);
		QpTupleBag<Integer> removeList = new QpTupleBag<Integer>(r, schemas, mdf);
		removeList.add(remove);
		agg.receiveTuples(null, removeList);
		agg.inputHasFinished(null, 0);
		
		
		
		QpTuple<Null> t17 = new QpTuple<Null>(t, new Object[] {1,7});
		
		List<QpTuple<Null>> result = spool.getResultsWithoutMetadata();
		assertEquals("Wrong number of results", 1, result.size());
		assertEquals("Incorrect result", t17, result.get(0));
		
		
	}
}
