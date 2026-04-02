package edu.upenn.cis.orchestra.optimization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.experlog.zql.ZQuery;
import com.experlog.zql.ZqlParser;

import edu.upenn.cis.orchestra.optimization.RelationTypes.MaterializedView;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;

public class TestSubexpressionGeneration {
	HashMapRelationTypes<Location,QpSchema> types = new HashMapRelationTypes<Location,QpSchema>();
	Optimizer<Location,QueryPlan<Null>,Integer,QpSchema> o;
	QueryPlanGenerator<Location,Integer,QpSchema,QueryPlan<Null>> qpg;
	static ZqlParser parser = new ZqlParser();

	@Before
	public void setUp() throws Exception {
		QpSchema s = new QpSchema("S", 2);
		s.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		s.addCol("c", edu.upenn.cis.orchestra.datamodel.DoubleType.DOUBLE);
		s.addCol("e", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		s.addCol("f", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		s.setPrimaryKey(Collections.singleton("a"));
		s.markFinished();

		QpSchema r = new QpSchema("R", 1);
		r.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.addCol("b", new edu.upenn.cis.orchestra.datamodel.StringType(true, true, true, 10));
		r.addCol("d", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.setPrimaryKey(Collections.singleton("a"));
		r.addForeignKey("fk1", Collections.singletonList("a"), s, Collections.singletonList("a"));
		r.markFinished();


		List<Integer> intBucketEdges = new ArrayList<Integer>();
		intBucketEdges.add(0);
		intBucketEdges.add(100);
		List<String> stringBucketEdges = new ArrayList<String>();
		stringBucketEdges.add("AA");
		stringBucketEdges.add("ZZ");
		List<Double> doubleBucketEdges = new ArrayList<Double>();
		doubleBucketEdges.add(0.0);
		doubleBucketEdges.add(100.0);

		double[] one = {1.0};

		Histogram<Integer> intHist = GenericHistogram.createIntegerHistogram(intBucketEdges, one, one);
		Histogram<String> stringHist = GenericHistogram.createStringHistogram(2, stringBucketEdges, one, one);
		Histogram<Double> doubleHist = GenericHistogram.createDoubleHistogram(doubleBucketEdges, one, one);

		List<Histogram<?>> rHists = new ArrayList<Histogram<?>>();
		rHists.add(intHist);
		rHists.add(stringHist);
		rHists.add(doubleHist);

		List<Histogram<?>> sHists = new ArrayList<Histogram<?>>();
		sHists.add(intHist);
		sHists.add(doubleHist);
		sHists.add(intHist);
		sHists.add(intHist);

		types.addRelation(s, Location.CENTRALIZED, sHists);
		types.addRelation(r, Location.CENTRALIZED, rHists);

		qpg = new QueryPlanGenerator<Location,Integer,QpSchema,QueryPlan<Null>>() {

			public Integer addTogether(Integer c1, Integer c2) {
				return c1 + c2;
			}

			public Integer getIdentity() {
				return 0;
			}

			public Collection<? extends edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.LocalCost<Integer, Location, QpSchema, QueryPlan<Null>>> getLocalCost(
					AndNode an,
					Location props,
					int expId,
					Optimizer<? extends Location, QueryPlan<Null>, Integer, ? extends QpSchema> o,
					PhysicalPropertiesFactory<Location> propFactory) {
				return null;
			}

			public LocalCost<Integer, Location, QpSchema, QueryPlan<Null>> getScanCost(
					int expId,
					Location props,
					Optimizer<? extends Location, QueryPlan<Null>, Integer, ? extends QpSchema> o) {
				return null;
			}

			public Integer subtractFrom(Integer c1, Integer c2) {
				return c1 - c2;
			}

			public boolean takeMaxOfMultipleInputs() {
				return false;
			}

			public int compare(Integer arg0, Integer arg1) {
				return arg0.compareTo(arg1);
			}

			public CreatedQP<QpSchema, QueryPlan<Null>, Integer> createQueryRoot(
					List<Variable> head,
					QpSchema headSchema,
					SchemaFactory<? extends QpSchema> schemaFactory,
					OperatorIdSource ois,
					Location dest,
					CreatedQP<QpSchema, QueryPlan<Null>, Integer> input) {
				return null;
			}

			public void setExpectedCard(QueryPlan<Null> plan, double card) {
			}
		};

		o  = new Optimizer<Location,QueryPlan<Null>,Integer,QpSchema>(1, true, types, qpg, null);
	}

	@Test
	public void testSimpleJoin() throws Exception {
		String query = "SELECT r.a, b, c FROM R r, S s WHERE r.a = s.a;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<Query> qs = new ArrayList<Query>();
		qs.add(q);

		// No repeated atoms => can ignore morphism
		List<AndNode> on = o.getOrNode(q.exp, null, true).andNodes;
		assertEquals(1, on.size());
		AndNode an = on.get(0);
		assertTrue(an instanceof JoinNode);
		JoinNode jn = (JoinNode) an;

		Set<EquivClass> noEcs = Collections.emptySet();
		Set<Variable> noGroupBy = null;
		Set<Predicate> noPreds = Collections.emptySet();
		Set<Function> noFuncs = Collections.emptySet();
		Set<Aggregate> noAggs = Collections.emptySet();
		Set<Variable> rHead = new HashSet<Variable>();
		rHead.add(new AtomVariable("R", 1, 0, types));
		rHead.add(new AtomVariable("R", 1, 1, types));
		Set<Variable> sHead = new HashSet<Variable>();
		sHead.add(new AtomVariable("S", 1, 0, types));
		sHead.add(new AtomVariable("S", 1, 1, types));
		Expression rExp = new Expression(rHead, Collections.singletonMap("R", 1), noEcs, noGroupBy, noPreds, noFuncs, noAggs);
		Expression sExp = new Expression(sHead, Collections.singletonMap("S", 1), noEcs, noGroupBy, noPreds, noFuncs, noAggs);
		Expression lhs = o.getExpressionForId(jn.lhs.expId), rhs = o.getExpressionForId(jn.rhs.expId);
		assertTrue("Couldn't find R expression in join", lhs.equals(rExp) || rhs.equals(rExp));
		assertTrue("Couldn't find S expression in join", lhs.equals(sExp) || rhs.equals(sExp));
		assertFalse("Join missing R expression or S expression", rhs.equals(lhs));
		assertEquals(1,jn.lhsJoinVars.size());
		assertEquals(1,jn.rhsJoinVars.size());
		Morphism mR = new Morphism(), mS = new Morphism();
		mR.mapOccurrence("R", 1, 1);
		mS.mapOccurrence("S", 1, 1);
		List<Variable> rJoinVars = Collections.singletonList((Variable) new AtomVariable("R", 1, 0, types));
		List<Variable> sJoinVars = Collections.singletonList((Variable) new AtomVariable("S", 1, 0, types));
		if (lhs.equals(rExp)) {
			assertEquals(mR, jn.lhsMap);
			assertEquals(mS, jn.rhsMap);
			assertEquals(jn.lhsJoinVars, rJoinVars);
			assertEquals(jn.rhsJoinVars, sJoinVars);
			assertEquals(rHead, lhs.head);
			assertEquals(sHead, rhs.head);			
		} else {
			assertEquals(mS, jn.lhsMap);
			assertEquals(mR, jn.rhsMap);
			assertEquals(jn.lhsJoinVars, sJoinVars);
			assertEquals(jn.rhsJoinVars, rJoinVars);
			assertEquals(sHead, lhs.head);
			assertEquals(rHead, rhs.head);			
		}
	}

	@Test
	public void testFunctionEvaluation() throws Exception {
		String query = "SELECT r.a, d + e FROM R r, S s WHERE r.a = s.a;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		List<AndNode> on = o.getOrNode(q.exp, null, true).andNodes;
		assertEquals(1, on.size());
		AndNode an = on.get(0);
		assertTrue(an instanceof FunctionNode);
	}

	@Test
	public void testFunctionPushdown() throws Exception {
		String query = "SELECT r.a, r.a+d, s.a+e FROM R r, S s WHERE r.a = s.a;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp, null, true).andNodes;

		assertEquals(4, on.size());
		boolean foundJoin = false;
		boolean foundBothFuncs = false;
		boolean foundRFunc = false;
		boolean foundSFunc = false;

		Set<Variable> rVars = new HashSet<Variable>(2);
		Set<Variable> sVars = new HashSet<Variable>(2);
		EquivClass ec = new EquivClass();
		ec.add(new AtomVariable("R", 1, 0, types));
		ec.add(new AtomVariable("S", 1, 0, types));
		ec.setFinished();
		rVars.add(ec);
		rVars.add(new AtomVariable("R", 1, 2, types));
		sVars.add(ec);
		sVars.add(new AtomVariable("S", 1, 2, types));


		Variable rFunc = Sum.create(rVars);
		Variable sFunc = Sum.create(sVars);

		for (AndNode andNode : on) {
			if (andNode instanceof JoinNode) {
				foundJoin = true;
			} else if (andNode instanceof FunctionNode) {
				FunctionNode fn = (FunctionNode) andNode;
				if (fn.functions.size() == 2) {
					foundBothFuncs = true;
				} else if (fn.functions.size() == 1) {
					for (Function f : fn.functions) {
						if (f.equals(rFunc)) {
							foundRFunc = true;
						} else if (f.equals(sFunc)) {
							foundSFunc = true;
						}
					}
				} else {
					fail("Found function node with impossible number of functions");
				}
			}
		}

		assertTrue("Missing join node", foundJoin);
		assertTrue("Missing 2-function node", foundBothFuncs);
		assertTrue("Missing function node for R sum", foundRFunc);
		assertTrue("Missing function node for S sum", foundSFunc);
	}

	@Test
	public void testPredicateEvaluation() throws Exception {
		String query = "SELECT r.a FROM R r, S s WHERE r.a = s.a AND d > e;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp, null, true).andNodes;
		assertEquals("Too many AndNodes", 1, on.size());
		assertTrue("Expected join node", on.get(0) instanceof JoinNode);
		assertNotNull("Expected predicate at join node", on.get(0).predicates);
	}

	@Test
	public void testPredicatePushdown() throws Exception {
		String query = "SELECT r.a, b FROM R r, S s WHERE r.a = s.a AND f < e;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals("Wrong number of and nodes", 1, on.size());
		assertTrue("Expected join", on.get(0) instanceof JoinNode);
		assertNull("Predicate must be pushed down", on.get(0).predicates);

		query = "SELECT r.a, b FROM R r, S s WHERE r.a = s.a AND r.a < 5;";
		parser.initParser(new StringReader(query));
		zq = (ZQuery) parser.readStatement();
		q = new Query(zq, types);
		on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals("Wrong number of and nodes", 1, on.size());
		assertTrue("Expected join node", on.get(0) instanceof JoinNode);

		JoinNode jn = (JoinNode) on.get(0);


		Expression lhs = o.getExpressionForId(jn.lhs.expId);
		Expression rhs = o.getExpressionForId(jn.rhs.expId);
		assertEquals("Should have one predicate on left side", 1, lhs.predicates.size());
		assertEquals("Should have one predicate on right side", 1, rhs.predicates.size());

		assertTrue("Missing relation R", lhs.relAtoms.containsKey("R") || rhs.relAtoms.containsKey("R"));
		assertTrue("Missing relation S", lhs.relAtoms.containsKey("S") || rhs.relAtoms.containsKey("S"));
		assertEquals("Wrong number of relations on left side", 1, lhs.relAtoms.size());
		assertEquals("Wrong number of relations on right side", 1, rhs.relAtoms.size());

		if (lhs.relAtoms.containsKey("R")) {
			assertEquals("Wrong number of occurrences of R", 1, (int) lhs.relAtoms.get("R"));
			assertEquals("Wrong number of occurrences of S", 1, (int) rhs.relAtoms.get("S"));
		} else {
			assertEquals("Wrong number of occurrences of R", 1, (int) rhs.relAtoms.get("R"));
			assertEquals("Wrong number of occurrences of S", 1, (int) lhs.relAtoms.get("S"));
		}

	}

	@Test
	public void testSimpleAggregate() throws Exception {
		String query = "SELECT MAX(a) FROM R GROUP BY b;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp, null, true).andNodes;
		assertEquals("Too many and nodes", 1, on.size());

		AndNode an = on.get(0);
		assertTrue("Top-level operator must be aggregation", an instanceof AggregateNode);
		AggregateNode agg = (AggregateNode) an;

		Aggregate av = Aggregate.makeAggregate("MAX", new AtomVariable("R", 1, 0, types));

		assertEquals("Wrong number of  aggregate variables", 1, agg.aggregates.size());
		assertTrue("Missing aggregate variable", agg.aggregates.contains(av));
		assertEquals("Incorrect grouping variables", agg.groupingVariables, Collections.singleton(new AtomVariable("R", 1, 1, types)));
	}

	@Test
	public void testAggregationWithPredicate() throws Exception {
		String query = "SELECT MAX(a) FROM R WHERE d < 5 GROUP BY b;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals("Too many and nodes", 1, on.size());

		AndNode an = on.get(0);
		assertTrue("Top-level operator must be aggregation", an instanceof AggregateNode);
		AggregateNode agg = (AggregateNode) an;
		Expression e = o.getExpressionForId(agg.input.expId);
		assertTrue("Subexpression should be a scan with a predicate", e.isScan() && (!e.predicates.isEmpty()));
	}

	@Test
	public void testAggregationPredicatePushdown() throws Exception {
		String query = "SELECT MAX(a) FROM R WHERE b <> 'Hello' GROUP BY b;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals("Expected only one node", 1, on.size());

		AndNode an = on.get(0);
		assertTrue("Expected aggregate node", an instanceof AggregateNode);
		AggregateNode agg = (AggregateNode) an;
		Expression aggSubExp = o.getExpressionForId(agg.input.expId);
		assertTrue("Child of aggregate node should be a scan with selection", aggSubExp.isScan() && (! aggSubExp.predicates.isEmpty()));

	}

	@Test
	public void testAggregationWithHaving() throws Exception {
		String query = "SELECT b FROM R GROUP BY b HAVING AVG(a) > AVG(d);";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals(1, on.size());
		assertTrue(on.get(0) instanceof AggregateNode);

		AggregateNode an = (AggregateNode) on.get(0);
		assertEquals("Too many predicates in aggregate node", 1, an.predicates.size());
		Predicate p = new Predicate(Aggregate.makeAggregate("AVG", new AtomVariable("R",1,2,types)), Predicate.Op.LT, Aggregate.makeAggregate("AVG", new AtomVariable("R",1,0,types)));
		assertEquals("Incorrect predicate in aggregate node", Collections.singleton(p), an.predicates);

		assertEquals("Incorrect grouping attributes", Collections.singleton(new AtomVariable("R",1,1,types)), an.groupingVariables);
		assertEquals("Incorrect atoms in subexpression", Collections.singletonMap("R", 1), o.getExpressionForId(an.input.expId).relAtoms);
		assertTrue("Input to aggregate should be a scan", o.getExpressionForId(an.input.expId).isScan());
	}
	
	@Test
	public void testAggregationWithFunction() throws Exception {
		String query = "SELECT MAX(d) + 1 FROM R GROUP BY b;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals(1, on.size());
		assertTrue(on.get(0) instanceof FunctionNode);

		FunctionNode fn = (FunctionNode) on.get(0);
		assertNull("Too many predicates in function node", fn.predicates);
		assertEquals("Wrong number of functions in function node", 1, fn.functions.size());
		Set<Variable> inputs = new HashSet<Variable>();
		inputs.add(new Aggregate(Aggregate.AggFunc.MAX,
				new AtomVariable("R", 1, 2, types)));
		inputs.add(new LiteralVariable(1));
		Variable f = Sum.create(inputs);
		assertEquals("Incorrect function in function node", Collections.singleton(f), fn.functions);
	}
	
	@Test
	public void testAggregationWithoutFKJoin() throws Exception {
		String query = "SELECT MAX(d), c FROM R, S WHERE R.a = S.a GROUP BY R.b, S.c;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals(1, on.size());
		assertTrue(on.get(0) instanceof AggregateNode);
	}
	
	@Test
	public void testAggregationWithFKJoin() throws Exception {
		String queryAndSubexps = "SELECT R.a, MAX(d), c FROM R, S WHERE R.a = S.a GROUP BY R.a, S.c; SELECT a, MAX(d) FROM R GROUP BY a; SELECT a, c FROM S;";
		parser.initParser(new StringReader(queryAndSubexps));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		zq = (ZQuery) parser.readStatement();
		Expression inputAgg = new Query(zq, types).exp;

		zq = (ZQuery) parser.readStatement();
		Expression inputExp = new Query(zq, types).exp;

		
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals("Wrong number of AndNodes", 2, on.size());
		
		boolean foundJoin = false, foundAgg = false;
		for (AndNode an : on) {
			if (an instanceof AggregateNode) {
				foundAgg = true;
			} else if (an instanceof JoinNode) {
				foundJoin = true;
				Expression foundInputAgg = null, foundInputExp = null;
				JoinNode jn = (JoinNode) an;
				Expression lhs = o.getExpressionForId(jn.lhs.expId);
				Expression rhs = o.getExpressionForId(jn.rhs.expId);
				if (lhs.groupBy == null) {
					foundInputExp = lhs;
					foundInputAgg = rhs;
				} else {
					foundInputExp = rhs;
					foundInputAgg = lhs;
				}
				assertEquals("Incorrect aggregated input to join node", inputAgg, foundInputAgg);
				assertEquals("Incorrect non-aggregated input to join node", inputExp, foundInputExp);
			} else {
				fail("Didn't expect AndNode " + an);
			}
		}
		if (! foundJoin) {
			fail("Missing join node");
		}
		if (! foundAgg) {
			fail("Missing aggregate node");
		}
	}

	@Test
	public void testAggregationWithFKJoinChangeHead() throws Exception {
		String queryAndSubexps = "SELECT MAX(d), c FROM R, S WHERE R.a = S.a GROUP BY R.a, S.c; SELECT a, MAX(d) FROM R GROUP BY a; SELECT a, c FROM S;";
		parser.initParser(new StringReader(queryAndSubexps));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		zq = (ZQuery) parser.readStatement();
		Expression inputAgg = new Query(zq, types).exp;

		zq = (ZQuery) parser.readStatement();
		Expression inputExp = new Query(zq, types).exp;

		
		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		assertEquals("Wrong number of AndNodes", 2, on.size());
		
		boolean foundJoin = false, foundAgg = false;
		for (AndNode an : on) {
			if (an instanceof AggregateNode) {
				foundAgg = true;
			} else if (an instanceof JoinNode) {
				foundJoin = true;
				Expression foundInputAgg = null, foundInputExp = null;
				JoinNode jn = (JoinNode) an;
				Expression lhs = o.getExpressionForId(jn.lhs.expId);
				Expression rhs = o.getExpressionForId(jn.rhs.expId);
				if (lhs.groupBy == null) {
					foundInputExp = lhs;
					foundInputAgg = rhs;
				} else {
					foundInputExp = rhs;
					foundInputAgg = lhs;
				}
				assertEquals("Incorrect aggregated input to join node", inputAgg, foundInputAgg);
				assertEquals("Incorrect non-aggregated input to join node", inputExp, foundInputExp);
			} else {
				fail("Didn't expect AndNode " + an);
			}
		}
		if (! foundJoin) {
			fail("Missing join node");
		}
		if (! foundAgg) {
			fail("Missing aggregate node");
		}
	}
	
	@Test
	public void testSimpleViewUse() throws Exception {
		String q1 = "SELECT a, d FROM R WHERE a < 6;";
		parser.initParser(new StringReader(q1));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		Expression exp = q.exp;
		QpSchema mv1 = new QpSchema("mv1", 3);
		mv1.markFinished();
		MaterializedView<Location,QpSchema> mv = new MaterializedView<Location,QpSchema>(exp, null, mv1, null);
		types.addMaterializedView(mv);
		o = new Optimizer<Location,QueryPlan<Null>,Integer,QpSchema>(1, true, types, qpg, null);

		String q2 = "SELECT a FROM R WHERE d < 5 AND a = d;";
		parser.initParser(new StringReader(q2));
		zq = (ZQuery) parser.readStatement();
		q = new Query(zq, types);

		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		// Since expression is a scan, should only get one node
		assertEquals("Missing view node",1, on.size());
		assertTrue("Expected only view nodes", on.get(0) instanceof ViewNode);
		ViewNode vn = (ViewNode) on.get(0);

		Predicate p1 = new Predicate(new AtomVariable("R", 1, 0, types), Predicate.Op.EQ, new AtomVariable("R", 1, 2, types));
		Predicate p2a = new Predicate(new AtomVariable("R", 1, 0, types), Predicate.Op.LT, new LiteralVariable(6));
		Predicate p2b = new Predicate(new AtomVariable("R", 1, 0, types), Predicate.Op.LT, new LiteralVariable(6));

		assertEquals("Wrong number of predicates", 2, vn.predicates.size());
		assertTrue("Missing equality predicate", vn.predicates.contains(p1));
		assertTrue("Missing inequality predicate", vn.predicates.contains(p2a) || vn.predicates.contains(p2b));
	}

	@Test
	public void testComplexViewUse() throws Exception {
		String q1 = "SELECT e, f FROM S WHERE 7 = e + f AND a = e;";
		parser.initParser(new StringReader(q1));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		Expression exp = q.exp;
		QpSchema mv1 = new QpSchema("mv1",3);
		mv1.markFinished();
		MaterializedView<Location,QpSchema> mv = new MaterializedView<Location,QpSchema>(exp, null, mv1, null);
		types.addMaterializedView(mv);
		o = new Optimizer<Location,QueryPlan<Null>,Integer,QpSchema>(1, true, types, qpg, null);

		String q2 = "SELECT e FROM S WHERE 7 = a + a AND a = e AND a = f;";
		parser.initParser(new StringReader(q2));
		zq = (ZQuery) parser.readStatement();
		q = new Query(zq, types);

		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		// Since expression is a scan, should only get one node
		assertEquals("Wrong number of nodes", 2, on.size());
		ViewNode vn = null;
		for (AndNode an : on) {
			if (an instanceof ViewNode) {
				if (vn == null) {
					vn = (ViewNode) an;
				} else {
					fail("Expected only one view node");
				}
			} else if (!(an instanceof FunctionNode)) {
				fail("Unexpected node " + on);
			}
		}
		assertNotNull("Missing view node", vn);

		EquivClass ec = new EquivClass();
		ec.add(new AtomVariable("S",1,0,types));
		ec.add(new AtomVariable("S",1,2,types));
		ec.setFinished();

		Predicate p1 = new Predicate(new AtomVariable("S", 1, 3, types), Predicate.Op.EQ, ec);

		assertNotNull("Missing view node predicates", vn.predicates);
		assertEquals("Wrong number of predicates", 1, vn.predicates.size());
		assertTrue("Missing equality predicate", vn.predicates.contains(p1));
	}

	@Test
	public void testAggregatedViewUse() throws Exception {
		String q1 = "SELECT a, MAX(c), MIN(f) FROM S GROUP BY a;";
		parser.initParser(new StringReader(q1));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		Expression exp = q.exp;
		QpSchema mv1 = new QpSchema("mv1",3);
		mv1.markFinished();
		MaterializedView<Location,QpSchema> mv = new MaterializedView<Location,QpSchema>(exp, null, mv1, null);
		types.addMaterializedView(mv);
		o = new Optimizer<Location,QueryPlan<Null>,Integer,QpSchema>(1, true, types, qpg, null);

		String q2 = "SELECT MAX(c) FROM S WHERE a = 5 GROUP BY a;";
		parser.initParser(new StringReader(q2));
		zq = (ZQuery) parser.readStatement();
		q = new Query(zq, types);

		List<AndNode> on = o.getOrNode(q.exp,null,true).andNodes;
		// Since expression is a scan, should only get one node
		assertEquals("Wrong number of nodes", 2, on.size());
		ViewNode vn = null;
		for (AndNode an : on) {
			if (an instanceof ViewNode) {
				if (vn == null) {
					vn = (ViewNode) an;
				} else {
					fail("Expected only one view node");
				}
			} else if (!(an instanceof AggregateNode)) {
				fail("Unexpected node " + on);
			}
		}
		assertNotNull("Missing view node", vn);

		assertNotNull("Missing predicate", vn.predicates);
		assertEquals("Wrong number of predicates", 1, vn.predicates.size());

		Set<Predicate> preds = Collections.singleton(new Predicate(new AtomVariable("S",1,0,types), Predicate.Op.EQ, new LiteralVariable(5)));
		assertEquals("Wrong predicates", preds, vn.predicates);
	}
}
