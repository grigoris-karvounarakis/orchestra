package edu.upenn.cis.orchestra.optimization;


import static org.junit.Assert.assertNotNull;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.DocumentBuilder;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.data.CreateQpSchema;
import edu.upenn.cis.data.TPCH;
import edu.upenn.cis.data.TestHarness;
import edu.upenn.cis.orchestra.optimization.QueryPlanGenerator.CreatedQP;
import edu.upenn.cis.orchestra.p2pqp.Bandwidth;
import edu.upenn.cis.orchestra.p2pqp.Null;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.SystemCalibration;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlanWithSchemas;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TestTPCHQueryOptimization {
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

	static final int numNodes = 10;
	Map<String,QpSchema> schemas;

	@Before
	public void setUp() throws Exception {
		docBuilder = javax.xml.parsers.DocumentBuilderFactory.newInstance().newDocumentBuilder();

		namedCal = Collections.emptyMap();
		namedBand = Collections.emptyMap();

		SystemCalibration localCal =  new SystemCalibration(167021,1021205,830456,211457360,364826718,8961433,9326661,1357122,3245850,466470,36598,259907,191513,64970,9150,176186,200705,2668988,2857660,314752);
		SystemCalibration remoteCal = new SystemCalibration( 85000, 500000,410000,100000000,18000000,4500000,4700000, 650000,1600000,230000,18000,130000, 85000,32000,4600, 90000,100000,1300000,1400000,160000);

		Map<String,Double> empty = Collections.emptyMap();

		localBand = new Bandwidth(empty, empty, 0x1000000, 0.005);
		remoteBand = new Bandwidth(empty, empty, 0x100000, 0.015);

		TPCH tpch = new TPCH("C:\\Users\\netaylor\\Documents\\Data\\tpch-1000");
		
		schemas = TPCH.createSchemas(new CreateQpSchema(TestHarness.hashCols));
		rt = tpch.createRT(schemas, null);

		lf = new Location.Factory();
		qpg = new P2PQPQueryPlanGenerator<QpSchema,Null>(10, localCal, remoteCal, namedCal, localBand, remoteBand, namedBand);
		optimizer = new Optimizer<Location,QueryPlan<Null>,Double,QpSchema>(1,true,rt,qpg,lf);
		qsf = new QpSchemaFactory(1);
		
		schemas = new HashMap<String,QpSchema>();
		for (String name : TPCH.tableNames) {
			schemas.put(name, rt.getBaseRelationSchema(name));
		}
	}

	@Test
	public void testQ1() throws Exception {
		Query q = TPCH.parsedQueries.get("Q1");
		QpSchema s = TPCH.querySchemas.get("Q1");
		
		long startTime = System.nanoTime();
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, s, Location.CENTRALIZED, qsf);
		long endTime = System.nanoTime();
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		int optTime = (int) ((endTime - startTime) / 1000000);
		
		System.out.println("Q1 ("+ optTime + " msec) :");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testQ3() throws Exception {
		Query q = TPCH.parsedQueries.get("Q3");
		QpSchema s = TPCH.querySchemas.get("Q3");
		
		long startTime = System.nanoTime();
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, s, Location.CENTRALIZED, qsf);
		long endTime = System.nanoTime();
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		int optTime = (int) ((endTime - startTime) / 1000000);
		
		System.out.println("Q3 ("+ optTime + " msec) :");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testQ5() throws Exception {
		Query q = TPCH.parsedQueries.get("Q5");
		QpSchema s = TPCH.querySchemas.get("Q5");
		
		long startTime = System.nanoTime();
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, s, Location.CENTRALIZED, qsf);
		long endTime = System.nanoTime();
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		int optTime = (int) ((endTime - startTime) / 1000000);
		
		System.out.println("Q5 ("+ optTime + " msec) :");
		DomUtils.write(doc, System.out);
	}
	
	@Test
	public void testQ6() throws Exception {
		Query q = TPCH.parsedQueries.get("Q6");
		QpSchema s = TPCH.querySchemas.get("Q6");
		
		long startTime = System.nanoTime();
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, s, Location.CENTRALIZED, qsf);
		long endTime = System.nanoTime();
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		int optTime = (int) ((endTime - startTime) / 1000000);
		
		System.out.println("Q6 ("+ optTime + " msec) :");
		DomUtils.write(doc, System.out);
	}

	@Test
	public void testQ10() throws Exception {
		Query q = TPCH.parsedQueries.get("Q10");
		QpSchema s = TPCH.querySchemas.get("Q10");
		
		long startTime = System.nanoTime();
		CreatedQP<QpSchema,QueryPlan<Null>,Double> cqp = optimizer.createQueryPlan(q, s, Location.CENTRALIZED, qsf);
		long endTime = System.nanoTime();
		assertNotNull("Created query must not be null", cqp.qp);
		
		QueryPlanWithSchemas<Null> qp = new QueryPlanWithSchemas<Null>(cqp.qp, qsf.getCreatedSchemasByName(), Null.class);
		Document doc = docBuilder.newDocument();
		Element el = doc.createElement("queryPlanAndSchema");
		qp.serialize(doc, el, schemas);
		doc.appendChild(el);
		
		int optTime = (int) ((endTime - startTime) / 1000000);
		
		System.out.println("Q10 ("+ optTime + " msec) :");
		DomUtils.write(doc, System.out);
	}
}
