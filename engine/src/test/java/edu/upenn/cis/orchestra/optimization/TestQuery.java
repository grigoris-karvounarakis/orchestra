package edu.upenn.cis.orchestra.optimization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.experlog.zql.ZQuery;
import com.experlog.zql.ZqlParser;

import edu.upenn.cis.orchestra.datamodel.Date;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;

public class TestQuery {
	HashMapRelationTypes<Location,QpSchema> types;
	static ZqlParser parser = new ZqlParser();

	@Before
	public void setUp() throws Exception {
		types = new HashMapRelationTypes<Location,QpSchema>();
		QpSchema r = new QpSchema("R", 1);
		r.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.addCol("b", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.addCol("c", edu.upenn.cis.orchestra.datamodel.DoubleType.DOUBLE);
		r.addCol("d", edu.upenn.cis.orchestra.datamodel.DateType.DATE);
		r.addCol("e", new edu.upenn.cis.orchestra.datamodel.StringType(true, true, false, 5));
		r.addCol("f", new edu.upenn.cis.orchestra.datamodel.StringType(true, true, true, 10));
		r.markFinished();

		QpSchema s = new QpSchema("S", 2);
		s.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		s.addCol("b", edu.upenn.cis.orchestra.datamodel.DoubleType.DOUBLE);
		s.markFinished();

		List<Integer> intBucketEdges = new ArrayList<Integer>();
		intBucketEdges.add(0);
		intBucketEdges.add(100);
		List<String> stringBucketEdges = new ArrayList<String>();
		stringBucketEdges.add("AA");
		stringBucketEdges.add("ZZ");
		List<Double> doubleBucketEdges = new ArrayList<Double>();
		doubleBucketEdges.add(0.0);
		doubleBucketEdges.add(100.0);
		List<Date> dateBucketEdges = new ArrayList<Date>();
		dateBucketEdges.add(new Date(1985,1,1));
		dateBucketEdges.add(new Date(2000,1,1));
		
		double[] one = {1.0};
		
		Histogram<Integer> intHist = GenericHistogram.createIntegerHistogram(intBucketEdges, one, one);
		Histogram<String> stringHist = GenericHistogram.createStringHistogram(2, stringBucketEdges, one, one);
		Histogram<Double> doubleHist = GenericHistogram.createDoubleHistogram(doubleBucketEdges, one, one);
		Histogram<Date> dateHist = GenericHistogram.createDateHistogram(dateBucketEdges, one, one);
		
		List<Histogram<?>> rHists = new ArrayList<Histogram<?>>();
		rHists.add(intHist);
		rHists.add(intHist);
		rHists.add(doubleHist);
		rHists.add(dateHist);
		rHists.add(stringHist);
		rHists.add(stringHist);
		
		List<Histogram<?>> sHists = new ArrayList<Histogram<?>>();
		sHists.add(intHist);
		sHists.add(doubleHist);

		types.addRelation(r, Location.CENTRALIZED, rHists);
		types.addRelation(s, Location.CENTRALIZED, sHists);
	}

	@Test
	public void testHead() throws Exception {
		String query = "SELECT d, S.b FROM R, S WHERE R.a = S.a;";
		parser.initParser(new StringReader(query));

		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		List<Variable> head = new ArrayList<Variable>();
		head.add(new AtomVariable("R", 1, 3, types));
		head.add(new AtomVariable("S", 1, 1, types));
		
		assertEquals(head, q.head);
		assertEquals(new HashSet<Variable>(head), q.exp.head);
	}
	
	@Test
	public void testFunctions() throws Exception {
		String query = "SELECT S.b * 3 * S.b / S.b FROM R, S WHERE R.a = S.a - 1 - 2;";
		parser.initParser(new StringReader(query));

		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		
		List<Variable> head = new ArrayList<Variable>();
		head.add(Product.create(Arrays.asList(new AtomVariable("S", 1, 1, types), new LiteralVariable(3))));
		assertEquals(head, q.head);
		
		EquivClass ec = new EquivClass();
		ec.add(new AtomVariable("R", 1, 0, types));
		ec.add(Sum.create(Arrays.asList(new AtomVariable("S", 1, 0, types), new LiteralVariable(-3)), Arrays.asList(1, 1)));
		ec.setFinished();
		assertEquals(Collections.singleton(ec), q.exp.equivClasses);
	}
	
	@Test
	public void testCreateExpression() throws Exception {
		String query = "SELECT a,c + 3.5,e || 'q',f FROM R WHERE a = 5 AND R.f = (e || 'de') AND b <> 17 AND d < DATE '2006-12-25';";
		parser.initParser(new StringReader(query));

		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		HashMap<String,Integer> relations = new HashMap<String,Integer>();
		relations.put("R", 1);				

		Set<Predicate> predicates = new HashSet<Predicate>();
		predicates.add(new Predicate(new AtomVariable("R",1,1,types), Predicate.Op.NE, new LiteralVariable(17)));
		predicates.add(new Predicate(new AtomVariable("R",1,3,types), Predicate.Op.LT, new LiteralVariable(new Date(2006,12,25))));

		List<Variable> funcInputs = new ArrayList<Variable>();

		Set<EquivClass> equivClasses = new HashSet<EquivClass>();

		EquivClass ec1 = new EquivClass();
		ec1.add(new AtomVariable("R", 1, 0, types));
		ec1.add(new LiteralVariable(5));
		ec1.setFinished();
		equivClasses.add(ec1);

		EquivClass ec2 = new EquivClass();
		ec2.add(new AtomVariable("R",1,5,types));
		funcInputs.clear();
		funcInputs.add(new AtomVariable("R",1,4,types));
		funcInputs.add(new LiteralVariable("de"));
		ec2.add(Concatenate.create(funcInputs));
		ec2.setFinished();
		equivClasses.add(ec2);

		List<Variable> head = new ArrayList<Variable>();
		head.add(ec1);

		funcInputs.clear();
		funcInputs.add(new AtomVariable("R", 1, 2, types));
		funcInputs.add(new LiteralVariable(3.5d));
		head.add(Sum.create(funcInputs));

		funcInputs.clear();
		funcInputs.add(new AtomVariable("R", 1, 4, types));
		funcInputs.add(new LiteralVariable("q"));
		head.add(Concatenate.create(funcInputs));

		head.add(ec2);


		assertEquals(head, q.head);
		assertEquals(relations, q.exp.relAtoms);
		assertEquals(predicates, q.exp.predicates);
		assertEquals(equivClasses,q.exp.equivClasses);
		assertNull(q.exp.groupBy);
	}

	@Test
	public void testCreateAggregateExpression() throws Exception {
		String query = "SELECT b, SUM(a) FROM R GROUP BY b HAVING SUM(a) > 12;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		HashMap<String,Integer> relations = new HashMap<String,Integer>();
		relations.put("R", 1);		

		List<Variable> head = new ArrayList<Variable>();
		head.add(new AtomVariable("R", 1, 1, types));
		Variable v = Aggregate.makeAggregate("SUM", new AtomVariable("R",1,0,types)); 
		head.add(v);

		Set<Variable> groupBy = new HashSet<Variable>();
		groupBy.add(new AtomVariable("R", 1, 1, types));

		Set<Predicate> predicates = new HashSet<Predicate>();
		predicates.add(new Predicate(new LiteralVariable(12), Predicate.Op.LT, v));

		assertEquals(0, q.exp.equivClasses.size());
		assertEquals(head,q.head);
		assertEquals(groupBy,q.exp.groupBy);
		assertEquals(predicates,q.exp.predicates);
		assertEquals(relations,q.exp.relAtoms);
	}
	
	@Test
	public void testCreateAggregateExpNoGroupBy() throws Exception {
		String query = "SELECT SUM(a) FROM R;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);
		
		HashMap<String,Integer> relations = new HashMap<String,Integer>();
		relations.put("R", 1);		

		List<Variable> head = new ArrayList<Variable>();
		Variable v = Aggregate.makeAggregate("SUM", new AtomVariable("R",1,0,types)); 
		head.add(v);

		assertEquals(0, q.exp.equivClasses.size());
		assertEquals(head,q.head);
		assertEquals(0,q.exp.groupBy.size());
		assertTrue(q.exp.predicates.isEmpty());
		assertEquals(relations,q.exp.relAtoms);

	}

	@Test
	public void testTypeError() throws Exception {
		String query = "SELECT a + d FROM R;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();
		boolean caughtError = false;

		try {
			new Query(zq, types);
		} catch (Type.TypeError te) {
			caughtError = true;
		}

		if (! caughtError) {
			fail("Adding an integer to a date did not cause a type error");
		}

		query = "SELECT a FROM R WHERE f = DATE '1996-02-21';";
		parser.initParser(new StringReader(query));
		zq = (ZQuery) parser.readStatement();
		caughtError = false;

		try {
			new Query(zq, types);
		} catch (Type.TypeError te) {
			caughtError = true;
		}

		if (! caughtError) {
			fail("Equating an integer and a date did not cause a type error");
		}
	}

	@Test
	public void testSyntaxError() throws Exception {
		String query = "SELECT a FROM R, R;";
		parser.initParser(new StringReader(query));
		ZQuery zq = (ZQuery) parser.readStatement();

		boolean caughtError = false;

		try {
			new Query(zq, types);
		} catch (Query.SyntaxError se) {
			caughtError = true;
		}

		if (! caughtError) {
			fail("Two instances of the same table without names did not cause an error");
		}

		query = "SELECT MAX(c), a FROM R GROUP BY b;";
		parser.initParser(new StringReader(query));
		zq = (ZQuery) parser.readStatement();
		caughtError = false;

		try {
			new Query(zq, types);
		} catch (Query.SyntaxError se) {
			caughtError = true;
		}

		if (! caughtError) {
			fail("Selecting non-aggregate non-grouping variable did not cause syntax error");
		}

		query = "SELECT MAX(a) FROM R GROUP BY b HAVING c < 3.0;";
		parser.initParser(new StringReader(query));
		zq = (ZQuery) parser.readStatement();
		caughtError = false;

		try {
			new Query(zq, types);
		} catch (Query.SyntaxError se) {
			caughtError = true;
		}

		if (! caughtError) {
			fail("Having clause refering to non-aggregate non-grouping variable did not cause syntax error");
		}

	}

	@Test
	public void testCreatePermutations() throws Exception {
		String query = "SELECT r1.a, r2.b, s1.a FROM R r1, R r2, R r3, S s1, S s2;";
		parser.initParser(new StringReader(query));

		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, types);

		String query2 = "SELECT r2.a, r1.b, s2.a FROM R r1, R r2, R r3, S s1, S s2;";
		parser.initParser(new StringReader(query2));

		ZQuery zq2 = (ZQuery) parser.readStatement();
		Query q2 = new Query(zq2, types);

		Set<Query> exps = q.computePermutations(types);
		assertEquals(12, exps.size());
		assertTrue(exps.contains(q));
		assertTrue(exps.contains(q2));
	}

	@Test
	public void testEquiv() throws Exception {
		String queries = "SELECT (r.a + 3) FROM R r, S s WHERE r.a = s.a; SELECT (s.a + 3) FROM R r, S s WHERE r.a = s.a; SELECT r.a FROM R r, S s WHERE r.a = s.a AND r.a > 3; SELECT r.a FROM R r, S s WHERE r.a = s.a AND s.a > 3; SELECT a FROM R where a = a+1;";
		parser.initParser(new StringReader(queries));

		ZQuery zq1 = (ZQuery) parser.readStatement(), zq2 = (ZQuery) parser.readStatement();
		Query q1 = new Query(zq1, types), q2 = new Query(zq2, types);		
		assertEquals("Queries should be equivalent", q1, q2);

		ZQuery zq3 = (ZQuery) parser.readStatement(), zq4 = (ZQuery) parser.readStatement();
		Query q3 = new Query(zq3, types), q4 = new Query(zq4, types);		
		assertEquals("Queries should be equivalent", q3, q4);
		
		ZQuery zq5 = (ZQuery) parser.readStatement();
		boolean caughtError = false;
		try {
			new Query(zq5, types);
		} catch (IllegalArgumentException iae) {
			caughtError = true;
		}
		assertTrue("Recursive equiv class did not cause exception", caughtError);
	}
}
