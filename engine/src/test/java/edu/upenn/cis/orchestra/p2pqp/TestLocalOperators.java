package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.datamodel.DateType;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnInput;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.ColumnOrFunction;
import edu.upenn.cis.orchestra.p2pqp.FunctionEvaluator.EvalFunc;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator.AggColumn;
import edu.upenn.cis.orchestra.p2pqp.HashAggregator.OutputColumn;
import edu.upenn.cis.orchestra.p2pqp.MaxIntForPredicate.IntPredPair;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.ComparePredicate.Op;

public class TestLocalOperators {
	private QpSchema r, s, t, u, v, w, x;
	private QpSchema.Source schemas;
	private Environment e;

	private QpTuple<Null> r1, r2, r3, r4, s1, t1, u1, u2, u3, u4, u5, v1, v2, v3, w1, w2, x1, x2, x4;
	QpTupleKey r1k, r2k, r3k, r4k, s1k, t1k, u1k, u2k, u3k, u4k, u5k, v1k, v2k, v3k, w1k, w2k, x1k, x2k, x4k;

	private InetSocketAddress nodeId;
	private final int queryId = 17;
	private final MetadataFactory<Null> mdf = null;
	private static final int phaseNo = 0;

	@Before
	public void setUp() throws Exception {
		nodeId = new InetSocketAddress(InetAddress.getLocalHost(), 5000);
		
		r = new QpSchema("R", 1);
		r.addCol("a", IntType.INT);
		r.addCol("b", new StringType(true, false, true, 10));
		r.setPrimaryKey(new PrimaryKey("pk", r, Collections.singleton("a")));
		r.markFinished();
		s = new QpSchema("S", 2);
		s.addCol("a", IntType.INT);
		s.addCol("c", DateType.DATE);
		s.setPrimaryKey(new PrimaryKey("pk", s, Collections.singleton("a")));
		s.markFinished();
		t = new QpSchema("T", 3);
		t.addCol("a", IntType.INT);
		t.addCol("b", new StringType(true, false, true, 10));
		t.addCol("c", DateType.DATE);
		t.setPrimaryKey(new PrimaryKey("pk", t, Collections.singleton("a")));
		t.markFinished();
		u = new QpSchema("U", 4);
		u.addCol("a", IntType.INT);
		u.addCol("b", IntType.INT);
		u.setPrimaryKey(new PrimaryKey("pk", u, Arrays.asList("a", "b")));
		u.markFinished();
		v = new QpSchema("V", 5);
		v.addCol("a", IntType.INT);
		v.addCol("count", IntType.INT);
		v.addCol("max", IntType.INT);
		v.addCol("min", IntType.INT);
		v.addCol("sum", IntType.INT);
		v.addCol("avg", IntType.INT);
		v.setPrimaryKey(new PrimaryKey("pk", v, Collections.singleton("a")));
		v.markFinished();
		w = new QpSchema("W", 6);
		w.addCol("a", new StringType(true, false, false, 1));
		w.addCol("b", new StringType(true, false, false,2));
		w.addCol("c", new StringType(true, false, true, 10));
		w.markFinished();
		x = new QpSchema("X", 7);
		x.addCol("a", IntType.INT);
		x.addCol("val", new IntType(false,false));
		x.setPrimaryKey(new PrimaryKey("pk", x, Collections.singleton("a")));
		x.markFinished();

		schemas = new QpSchema.CollectionSource(Arrays.asList(r, s, t, u, v, w, x));
		
		r1 = new QpTuple<Null>(r, new Object[] {5, "Hello!"});
		r1k = r1.getKeyTuple(2);
		r2 = new QpTuple<Null>(r, new Object[] {5, "Good-bye!"});
		r2k = r2.getKeyTuple(1);
		r3 = new QpTuple<Null>(r, new Object[] {7, null});
		r3k = r3.getKeyTuple(1);
		r4 = new QpTuple<Null>(r, new Object[] {6, "Ciao!"});
		r4k = r4.getKeyTuple(1);
		s1 = new QpTuple<Null>(s, new Object[] {5, new Date(1998,6,17)});
		s1k = s1.getKeyTuple(1);
		t1 = new QpTuple<Null>(t, new Object[] {5, "Hello!", s1.get("c")});
		t1k = t1.getKeyTuple(2);
		u1 = new QpTuple<Null>(u, new Object[] {1, 1});
		u1k = u1.getKeyTuple(1);
		u2 = new QpTuple<Null>(u, new Object[] {1, 3});
		u2k = u2.getKeyTuple(2);
		u3 = new QpTuple<Null>(u, new Object[] {2, 7});
		u3k = u3.getKeyTuple(3);
		u4 = new QpTuple<Null>(u, new Object[] {2, null});
		u4k = u4.getKeyTuple(4);
		u5 = new QpTuple<Null>(u, new Object[] {3, null});
		u5k = u5.getKeyTuple(5);
		v1 = new QpTuple<Null>(v, new Object[] {1,2,3,1,4,2});
		v1k = v1.getKeyTuple(2);
		v2 = new QpTuple<Null>(v, new Object[] {2,1,7,7,7,7});
		v2k = v2.getKeyTuple(4);
		v3 = new QpTuple<Null>(v, new Object[] {3,0,null,null,null,null});
		v3k = v3.getKeyTuple(5);
		w1 = new QpTuple<Null>(w, new Object[] {"a", "bb", null});
		w1k = w1.getKeyTuple(1);
		w2 = new QpTuple<Null>(w, new Object[] {"a", null, null});
		w2k = w2.getKeyTuple(2);
		x1 = new QpTuple<Null>(x, new Object[] {5, 5});
		x1k = x1.getKeyTuple(2);
		x2 = new QpTuple<Null>(x, new Object[] {6, 6});
		x2k = x2.getKeyTuple(1);
		x4 = new QpTuple<Null>(x, new Object[] {7, 0});
		x4k = x4.getKeyTuple(1);

		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				file.delete();
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		e = new Environment(f, ec);
	}

	@After
	public void tearDown() throws Exception {
		if (e != null) {
			e.close();
		}
	}
	
	@Test
	public void testJoinNoRecovery() throws Exception {
		testJoin(false);		
	}

	@Test
	public void testJoinWithRecovery() throws Exception {
		testJoin(true);
	}
	
	private void testJoin(boolean enableRecovery) throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		BDbTupleStore<Null> ts = new BDbTupleStore<Null>(e, "testJoin", mdf);

		try {
			ts.addTable(r);
			ts.addTable(s);
			ts.addTable(t);

			List<Integer> rOutputVariables = new ArrayList<Integer>(), sOutputVariables = new ArrayList<Integer>();
			rOutputVariables.add(0);
			rOutputVariables.add(1);
			sOutputVariables.add(null);
			sOutputVariables.add(2);

			ts.addTuple(r1, r1k.epoch);
			ts.addTuple(r2, r2k.epoch);
			ts.addTuple(s1, s1k.epoch);

			Map<Integer,int[]> recoveryOperators = null;
			if (enableRecovery) {
				recoveryOperators = Collections.emptyMap();
			}
			
			SpoolOperator<Null> spool = SpoolOperator.create(t, schemas, nodeId, 7, rt, mdf, enableRecovery, false);
			PipelinedHashJoin<Null> phj = new PipelinedHashJoin<Null>(r, s, Collections.singletonList(0), Collections.singletonList(0),
					rOutputVariables, sOutputVariables, t, spool, null, nodeId, 100, rt, mdf, schemas, recoveryOperators);

			PullScanOperator<Null> rScan = ts.beginScan("R", phj, WhichInput.LEFT, null, null, null, queryId, nodeId, 101, rt, enableRecovery, phaseNo);
			rScan.scanAll(0);
			PullScanOperator<Null> sScan = ts.beginScan("S", phj, WhichInput.RIGHT, null, null, null, queryId, nodeId, 102, rt, enableRecovery, phaseNo);
			sScan.scanAll(0);

			assertTrue("Should have received end-of-stream", spool.hasFinished());

			Set<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(spool.getResultsWithoutMetadata());
			assertEquals("Incorrect output from join operator", Collections.singleton(t1), output);
		} finally {
			ts.close();
		}
	}

	@Test
	public void testAggregatorNoRecovery() throws Exception {
		testAggregator(false);
	}
	
	@Test
	public void testAggregatorWithRecovery() throws Exception {
		testAggregator(true);
	}
	
	private void testAggregator(boolean enableRecovery) throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		SpoolOperator<Null> outputDest = SpoolOperator.create(v, schemas, nodeId, 9, rt, mdf, false, false);

		List<OutputColumn> ocs = new ArrayList<OutputColumn>();
		ocs.add(new OutputColumn(0));
		ocs.add(new AggColumn(1, AggFunc.COUNT));
		ocs.add(new AggColumn(1, AggFunc.MAX));
		ocs.add(new AggColumn(1, AggFunc.MIN));
		ocs.add(new AggColumn(1, AggFunc.SUM));
		ocs.add(new AggColumn(1, AggFunc.AVG));
		
		Map<Integer,int[]> recoveryOperators = null;
		if (enableRecovery) {
			recoveryOperators = Collections.emptyMap();
		}
		
		HashAggregator<Null> ha = new HashAggregator<Null>(outputDest, null, u, v, Collections.singletonList(0), ocs, nodeId, 200, rt, mdf, schemas, recoveryOperators, null);

		Set<QpTuple<Null>> result = new HashSet<QpTuple<Null>>();
		result.add(v1);
		result.add(v2);
		result.add(v3);

		
		List<QpTuple<Null>> input = new ArrayList<QpTuple<Null>>();
		input.add(u1);
		input.add(u2);
		input.add(u3);
		input.add(u4);
		input.add(u5);
		CollectionScan<Null> inputScan = new CollectionScan<Null>(u, null, input, ha, WhichInput.LEFT, queryId, nodeId, 201, rt, mdf, enableRecovery);
		inputScan.scanAll(0);

		assertTrue("Should have received end-of-stream", outputDest.hasFinished());
		HashSet<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(outputDest.getResultsWithoutMetadata());
		assertEquals("Incorrect result from aggregation", result, output);
	}

	@Test
	public void testFunctionEvaluatorConcatenateNoRecovery() throws Exception {
		testFunctionEvaluatorConcatenate(false);
	}
	
	@Test
	public void testFunctionEvaluatorConcatenateWithRecovery() throws Exception {
		testFunctionEvaluatorConcatenate(true);
	}
	
	private void testFunctionEvaluatorConcatenate(boolean enableRecovery) throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		SpoolOperator<Null> outputDest = SpoolOperator.create(u, schemas, nodeId, 11, rt, mdf, enableRecovery, false);
		List<String> between = new ArrayList<String>(3);
		between.add(null);
		between.add(null);
		between.add(null);
		List<Integer> lengths = new ArrayList<Integer>(2);
		lengths.add(1);
		lengths.add(2);

		List<ColumnInput> concInputs = new ArrayList<ColumnInput>(2);
		concInputs.add(new ColumnInput(0));
		concInputs.add(new ColumnInput(1));

		List<ColumnOrFunction> cofs = new ArrayList<ColumnOrFunction>();
		cofs.add(new ColumnInput(0));
		cofs.add(new ColumnInput(1));
		cofs.add(new EvalFunc(new Concatenate(true, true, between,lengths), concInputs));

		QpTupleBag<Null> inputTuples = new QpTupleBag<Null>(w, schemas, null);
		inputTuples.add(w1);
		inputTuples.add(w2);

		outputDest = SpoolOperator.create(w, schemas, nodeId, 11, rt, mdf, enableRecovery, false);
		FunctionEvaluator<Null> fe = new FunctionEvaluator<Null>(w, w, cofs, outputDest, WhichInput.LEFT, nodeId, 15, mdf, schemas, rt, enableRecovery);
		fe.receiveTuples(null, inputTuples);
		fe.inputHasFinished(null, 0);
		assertTrue("Should have received end-of-stream", outputDest.hasFinished());

		Set<QpTuple<Null>> outputTuples = new HashSet<QpTuple<Null>>();
		StringBuilder sb = new StringBuilder((String) w1.get(0));
		sb.append((String) w1.get(1));
		QpTuple<Null> w1out = new QpTuple<Null>(w, new Object[] {w1.get(0), w1.get(1), sb.toString()});
		outputTuples.add(w1out);
		outputTuples.add(w2);

		Set<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(outputDest.getResultsWithoutMetadata());
		assertEquals("Incorrect results from concatenate function", outputTuples, output);
	}
	
	@Test
	public void testFunctionEvaluatorSumNoRecovery() throws Exception {
		testFunctionEvaluatorSum(false);
	}
	
	@Test
	public void testFunctionEvaluatorSumWithRecovery() throws Exception {
		testFunctionEvaluatorSum(true);
	}

	private void testFunctionEvaluatorSum(boolean enableRecovery) throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		SpoolOperator<Null> outputDest = SpoolOperator.create(u, schemas, nodeId, 11, rt, mdf, enableRecovery, false);

		List<ColumnOrFunction> cofs = new ArrayList<ColumnOrFunction>();
		cofs.add(new ColumnInput(0));
		cofs.add(new EvalFunc(new Sum(Arrays.asList(1),true,false,false,1), Collections.singletonList(new EvalFunc(new Sum(Arrays.asList(1),true,false,false,2), Collections.singletonList(new ColumnInput(1))))));

		QpTupleBag<Null> input = new QpTupleBag<Null>(u, schemas, null);
		input.add(u2);
		FunctionEvaluator<Null> fe = new FunctionEvaluator<Null>(u, u, cofs, outputDest, WhichInput.LEFT, nodeId, 13, mdf, schemas, rt, enableRecovery);
		fe.receiveTuples(null, input);
		fe.inputHasFinished(null, 0);
		assertTrue("Should have received end-of-stream", outputDest.hasFinished());

		QpTuple<?> u2out = new QpTuple<Null>(u, new Object[] {u2.get(0), ((Integer) u2.get(1)) + 3});

		Set<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(outputDest.getResultsWithoutMetadata());
		assertEquals("Incorrect results from sum function", Collections.singleton(u2out), output);

		QpTuple<?> u4out = new QpTuple<Null>(u, new Object[] {u4.get(0), null});

		input.clear();
		input.add(u4);
		outputDest = SpoolOperator.create(u, schemas, nodeId, 11, rt, mdf, enableRecovery, false);
		fe = new FunctionEvaluator<Null>(u, u, cofs, outputDest, WhichInput.LEFT, nodeId, 13, mdf, schemas, rt, enableRecovery);
		fe.receiveTuples(null, input);
		fe.inputHasFinished(null, 0);
		assertTrue("Should have received end-of-stream", outputDest.hasFinished());
		output = new HashSet<QpTuple<Null>>(outputDest.getResultsWithoutMetadata());
		assertEquals("Incorrect results from sum function with null", Collections.singleton(u4out), output);
	}
	
	@Test
	public void testFunctionEvaluatorPredNoRecovery() throws Exception {
		testFunctionEvaluatorPred(false);
	}
	
	@Test
	public void testFunctionEvaluatorPredWithRecovery() throws Exception {
		testFunctionEvaluatorPred(true);
	}

	private void testFunctionEvaluatorPred(boolean enableRecovery) throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		QpTupleBag<Null> inputTuples = new QpTupleBag<Null>(r, schemas, mdf);
		inputTuples.add(r4);
		inputTuples.add(r2);
		inputTuples.add(r3);
		Set<QpTuple<Null>> outputTuples = new HashSet<QpTuple<Null>>();
		outputTuples.add(x1);
		outputTuples.add(x2);
		outputTuples.add(x4);

		Predicate p5 = ComparePredicate.createColLit(r, 0, Op.EQ, 5), p6 = ComparePredicate.createColLit(r, 0, Op.EQ, 6);
		List<IntPredPair> ipps = new ArrayList<IntPredPair>(2);
		ipps.add(new IntPredPair(5, p5));
		ipps.add(new IntPredPair(6, p6));
		MaxIntForPredicate mifp = new MaxIntForPredicate(r, ipps);
		List<ColumnInput> cis = new ArrayList<ColumnInput>();
		cis.add(new ColumnInput(0));
		cis.add(new ColumnInput(1));

		List<ColumnOrFunction> cofs = new ArrayList<ColumnOrFunction>();
		cofs.add(new ColumnInput(0));
		cofs.add(new EvalFunc(mifp, cis));

		SpoolOperator<Null> outputDest = SpoolOperator.create(x, schemas, nodeId, 11, rt, mdf, enableRecovery, false);
		FunctionEvaluator<Null> fe = new FunctionEvaluator<Null>(r,x, cofs, outputDest, WhichInput.LEFT, nodeId, 17, mdf, schemas, rt, enableRecovery);
		fe.receiveTuples(null, inputTuples);
		fe.inputHasFinished(null, 0);
		assertTrue("Should have received end-of-stream", outputDest.hasFinished());
		Set<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(outputDest.getResultsWithoutMetadata());
		assertEquals("Incorrect results from pred function", outputTuples, output);	
	}
	@Test
	public void testSelectionNoRecovery() throws Exception {
		testSelection(false);
	}
	
	@Test
	public void testSelectionWithRecovery() throws Exception {
		testSelection(true);
	}
	
	private void testSelection(boolean enableRecovery) throws Exception {
		RecordTuplesTest rt = new RecordTuplesTest();
		Set<QpTuple<Null>> input = new HashSet<QpTuple<Null>>();
		input.add(r4);
		input.add(r1);
		
		SpoolOperator<Null> outputOp = SpoolOperator.create(r, schemas, nodeId, 21, rt, mdf, enableRecovery, false);
		
		FilterOperator<Null> f = new FilterOperator<Null>(ComparePredicate.createColLit(r, "a", Op.NE, 5), outputOp, null, nodeId, 500, mdf, schemas, rt, false);
		//CollectionScan(QpSchema schema, QpSchema.Source findSchema, Collection<QpTuple<M>> tuples, Operator<M> dest, WhichInput outputDest, int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf) {
		PullScanOperator<Null> so = new CollectionScan<Null>(r, schemas, input,f,WhichInput.LEFT,queryId,nodeId,23,rt,mdf,enableRecovery);
		so.scanAll(0);
		assertTrue("Should have received end-of-stream", outputOp.hasFinished());
		
		Set<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(outputOp.getResultsWithoutMetadata());
		assertEquals("Wrong output tuples from selection", Collections.singleton(r4), output);
	}
	
	@Test
	public void testJoinNoRecoveryWithProjection() throws Exception {
		testJoinWithProjection(false);
	}
	
	@Test
	public void testJoinWithRecoveryAndProjection() throws Exception {
		testJoinWithProjection(true);
	}
	
	private void testJoinWithProjection(boolean enableRecovery) throws Exception {
		Map<Integer,int[]> recoveryOperators = null;
		if (enableRecovery) {
			recoveryOperators = Collections.emptyMap();
		}
		
		QpSchema a = new QpSchema("A", 1);
		final IntType intType = new IntType(false, false); 
		a.addCol("a", intType);
		a.addCol("b", intType);
		a.addCol("c", intType);
		a.markFinished();
		QpSchema b = new QpSchema("B", 2);
		b.addCol("a", intType);
		b.addCol("d", intType);
		b.addCol("e", intType);
		b.markFinished();
		QpSchema c = new QpSchema("C", 3);
		c.addCol("b", intType);
		c.addCol("d", intType);
		c.markFinished();
		
		List<QpTuple<Null>> aInput = new ArrayList<QpTuple<Null>>();
		List<QpTuple<Null>> bInput = new ArrayList<QpTuple<Null>>();
		Set<QpTuple<Null>> cOutput = new HashSet<QpTuple<Null>>();

		aInput.add(new QpTuple<Null>(a, new Object[] {1, 2, 3}));
		bInput.add(new QpTuple<Null>(b, new Object[] {1, 4, 5}));
		cOutput.add(new QpTuple<Null>(c, new Object[] {2, 4}));
		
		aInput.add(new QpTuple<Null>(a, new Object[] {11, 12, 23}));
		bInput.add(new QpTuple<Null>(b, new Object[] {11, 14, 15}));
		cOutput.add(new QpTuple<Null>(c, new Object[] {12, 14}));
		RecordTuplesTest rt = new RecordTuplesTest();
		
		List<Integer> aOutputVariables = Arrays.asList(-1, 0, -1);
		List<Integer> bOutputVariables = Arrays.asList(-1, 1, -1);
		
		SpoolOperator<Null> spool = SpoolOperator.create(c, schemas, nodeId, 21, rt, mdf, enableRecovery, false);
		PipelinedHashJoin<Null> phj = new PipelinedHashJoin<Null>(a, b, Collections.singletonList(0), Collections.singletonList(0),
				aOutputVariables, bOutputVariables, c, spool, null, nodeId, 100, rt, mdf, schemas, recoveryOperators);

		PullScanOperator<Null> aScan = new CollectionScan<Null>(a, schemas, aInput, phj, WhichInput.LEFT,queryId,nodeId,23,rt,mdf,enableRecovery);
		aScan.scanAll(0);
		PullScanOperator<Null> bScan = new CollectionScan<Null>(b, schemas, bInput, phj, WhichInput.RIGHT,queryId,nodeId,24,rt,mdf,enableRecovery);
		bScan.scanAll(0);

		assertTrue("Should have received end-of-stream", spool.hasFinished());

		Set<QpTuple<Null>> output = new HashSet<QpTuple<Null>>(spool.getResultsWithoutMetadata());
		assertEquals("Incorrect output from join operator", cOutput, output);
	}
}
