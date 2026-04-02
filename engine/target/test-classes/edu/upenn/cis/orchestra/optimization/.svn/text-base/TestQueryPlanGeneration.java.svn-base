package edu.upenn.cis.orchestra.optimization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.experlog.zql.ParseException;
import com.experlog.zql.ZQuery;
import com.experlog.zql.ZqlParser;

import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.optimization.Query.SyntaxError;
import edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.CreatedQP;
import edu.upenn.cis.orchestra.optimization.RelationTypes.MaterializedView;
import edu.upenn.cis.orchestra.optimization.Type.TypeError;
import edu.upenn.cis.orchestra.p2pqp.Bandwidth;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.SystemCalibration;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlanWithSchemas;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TestQueryPlanGeneration {
	DocumentBuilder docBuilder;

	SystemCalibration localCal, remoteCal;
	Bandwidth localBand, remoteBand;
	Map<String,SystemCalibration> namedCal;
	Map<String,Bandwidth> namedBand;
	
	
	Optimizer<Location,QueryPlan<Null>,Double,QpSchema> optimizer;
	P2PQPQueryPlanGenerator<QpSchema,Null> qpg;
	HashMapRelationTypes<Location,QpSchema> rt;
	Location.Factory lf;
	
	QpSchemaFactory qsf;
	Map<String,QpSchema> schemas;
	
	static ZqlParser parser = new ZqlParser();
	static final int numNodes = 10;
	
	
	@Before
	public void setUp() throws Exception {
		docBuilder = javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder();

		SystemCalibration localCal =  new SystemCalibration(167021,1021205,830456,211457360,36482618,8961433,9326661,1357122,3245850,466470,36598,259907,191513,64970,9150,176186,200705,2668988,2857660,314752);
		SystemCalibration remoteCal = new SystemCalibration( 85000, 500000,410000,100000000,18000000,4500000,4700000, 650000,1600000,230000,18000,130000, 85000,32000,4600, 90000,100000,1300000,1400000,160000);
		namedCal = Collections.singletonMap("mv1", remoteCal);

		Map<String,Double> empty = Collections.emptyMap();
		Map<String,Double> bandToNamed = Collections.singletonMap("mv1", (double) 0x5000000);
		Map<String,Double> latencyToNamed = Collections.singletonMap("mv1", 0.003);
		
		localBand = new Bandwidth(bandToNamed, latencyToNamed, 0x1000000, 0.005);
		bandToNamed = Collections.singletonMap("mv1", (double) 0x2000000);
		latencyToNamed = Collections.singletonMap("mv1", 0.007);
		remoteBand = new Bandwidth(bandToNamed, latencyToNamed, 0x100000, 0.015);
		namedBand = Collections.singletonMap("mv1", new Bandwidth(empty, empty, 0x1000000, 0.005));
		
		rt = new HashMapRelationTypes<Location,QpSchema>();
		
		QpSchema r = new QpSchema("R", 1);
		r.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.addCol("b", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		r.setPrimaryKey(Arrays.asList("a"));
		r.setHashCols(new int[] {0});
		r.markFinished();
		
		List<Histogram<?>> rHists = new ArrayList<Histogram<?>>(2);
		rHists.add(Histogram.createIntegerHistogram(Arrays.asList(1,6,11), new double[] {5.0, 2.0}, new double[] {3.0,1.0}));
		rHists.add(Histogram.createIntegerHistogram(Arrays.asList(1,6,11), new double[] {5.0, 2.0}, new double[] {3.0,1.0}));
		rt.addRelation(r, r.getOptimizerLocation(), rHists);

		QpSchema s = new QpSchema("S", 2);
		s.addCol("a", edu.upenn.cis.orchestra.datamodel.IntType.INT);
		s.addCol("c", new StringType(true,false,true,20));
		s.setPrimaryKey(Arrays.asList("a","c"));
		s.addForeignKey("fk1", Arrays.asList("a"), r, Arrays.asList("a"));
		s.setHashCols(new int[] {0});
		s.markFinished();

		List<Histogram<?>> sHists = new ArrayList<Histogram<?>>(2);
		sHists.add(Histogram.createIntegerHistogram(Arrays.asList(1,6,11), new double[] {5.0, 3.0}, new double[] {2.0,1.0}));
		sHists.add(Histogram.createStringHistogram(3,
				Arrays.asList(
				Histogram.convertForHistogram(3, "Albert"),
				Histogram.getSuccessorForHistogram(3, "Zebediah")),
				new double[] {8.0}, new double[] { 6.0 }));
		rt.addRelation(s, s.getOptimizerLocation(), sHists);
		
		lf = new Location.Factory();
		qpg = new P2PQPQueryPlanGenerator<QpSchema,Null>(10, localCal, remoteCal, namedCal, localBand, remoteBand, namedBand);
		optimizer = new Optimizer<Location,QueryPlan<Null>,Double,QpSchema>(1,true,rt,qpg,lf);
		qsf = new QpSchemaFactory(1);
		
		schemas = new HashMap<String,QpSchema>();
		schemas.put("R", r);
		schemas.put("S", s);
	}
	
	@Test
	public void testFunctionOptimization() throws Exception {
		Query q = getQuery("SELECT a + 3 FROM R");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testFunctionOptimization");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testJoinOptimization() throws Exception {
		Query q = getQuery("SELECT R.a, S.c FROM R, S WHERE R.a = S.a");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testJoinQueryOptimization");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testSimpleOptimizationWithPredicate() throws Exception {
		Query q = getQuery("SELECT R.b FROM R WHERE R.a = 5");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testSimpleOptimizationWithPredicate");
		DomUtils.write(doc, System.out);
	}

	
	@Test
	public void testJoinOptimizationWithPredicate() throws Exception {
		Query q = getQuery("SELECT R.a, R.b, S.c FROM R, S WHERE R.b = 5 AND R.a = S.a");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testJoinOptimizationWithPredicate");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testSimpleAggregateOptimization() throws Exception {
		Query q = getQuery("SELECT a, MAX(b) FROM R GROUP BY a");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testSimpleAggregateOptimization");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testAggregateWithJoinOptimization() throws Exception {
		Query q = getQuery("SELECT R.a, MAX(c) FROM R, S WHERE R.a = S.a GROUP BY R.a");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testAggregateWithJoinOptimization");
		DomUtils.write(doc, System.out);
	}

	@Test
	public void testAggregateWithPredicateOptimization() throws Exception {
		Query q = getQuery("SELECT a, MAX(b) FROM R WHERE a < 6 GROUP BY a HAVING MAX(b) > 10");
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testAggregateWithPredicateOptimization");
		DomUtils.write(doc, System.out);
	}

	@Test
	public void testViewOptimization() throws Exception {
		Query q = getQuery("SELECT R.a, b, c FROM R, S WHERE R.a = S.a");
		QpSchema s = new QpSchema("mv1", 3);
		s.addCol("C1", new edu.upenn.cis.orchestra.datamodel.IntType(true,false));
		s.addCol("C2", new edu.upenn.cis.orchestra.datamodel.IntType(true,false));
		s.addCol("C3", new StringType(true,false,true,20));
		s.setNamedLocation("mv1");
		s.markFinished();
		VariablePosition vp = new VariablePosition(3);
		EquivClass ec = new EquivClass();
		ec.add(new AtomVariable("R", 1, 0, rt));
		ec.add(new AtomVariable("S", 1, 0, rt));
		ec.setFinished();
		vp.addVariable(ec);
		vp.addVariable(new AtomVariable("R", 1, 1, rt));
		vp.addVariable(new AtomVariable("S", 1, 1, rt));
		vp.finish();
		MaterializedView<Location,QpSchema> mv =
			new MaterializedView<Location,QpSchema>(q.exp, new Location("mv1"), s, vp);
		rt.addMaterializedView(mv);
		
		optimizer = new Optimizer<Location,QueryPlan<Null>,Double,QpSchema>(1,true,rt,qpg,lf);

		q = getQuery("SELECT b+1, c FROM R,S WHERE R.a = S.a");
		
		List<AndNode> ans = optimizer.getOrNode(q.exp, null, true).andNodes;
		assertEquals("Wrong number of rewritings", 2, ans.size());
		boolean foundJoin = false, foundFunc = false;
		for (AndNode an : ans) {
			if (an instanceof FunctionNode) {
				foundFunc = true;
			} else if (an instanceof JoinNode) {
				foundJoin = true;
			} else {
				fail("Shouldn't have node " + an);
			}
		}
		assertTrue("Missing join node", foundJoin);
		assertTrue("Missing function node", foundFunc);
		
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, Location.CENTRALIZED, qsf);
		assertNotNull("Created query must not be null", cqp.qp);
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		schemas.put(s.getName(), s);
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		System.out.println("testViewOptimization");
		DomUtils.write(doc, System.out);

	}
	
	private Query getQuery(String SQL) throws ParseException, TypeError, SyntaxError {
		if (! SQL.endsWith(";")) {
			SQL = SQL + ";";
		}
		parser.initParser(new StringReader(SQL));
		ZQuery zq = (ZQuery) parser.readStatement();
		Query q = new Query(zq, rt);
		return q;
	}
}
