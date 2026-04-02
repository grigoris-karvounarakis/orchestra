package edu.upenn.cis.orchestra.datamodel;

import static org.junit.Assert.assertEquals;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.util.DomUtils;

public class TestSchemaTuple {
	Schema s;
	Relation r;
	Tuple tN1, tM, tAl;
	DocumentBuilder db;
	
	@Before
	public void setUp() throws Exception {
		s = new Schema();
		r = s.addRelation("R");
		r.addCol("name", new StringType(true, false,true, 10));
		r.addCol("val", new IntType(false,false));
		s.markFinished();

		tN1 = s.createTuple("R");
		tN1.set("name", "Nick");
		tN1.set("val", 1);
		tN1.setReadOnly();
		
		tM = s.createTuple("R");
		tM.set("name", "Mark");
		tM.setReadOnly();
		
		tAl = s.createTuple("R");
		tAl.set("name", "Ann");
		tAl.setLabeledNull("val", 17);
		
		db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
	}
	
	@Test
	public void testSimpleXML() throws Exception {
		Document d = db.newDocument();
		Element tuple = d.createElement("tuple");
		d.appendChild(tuple);
		
		tN1.serialize(d, tuple);
		
		DomUtils.write(d, System.out);
		
		Tuple tN1d = AbstractTuple.deserialize(tuple, s);
		assertEquals("Incorrect result from deserializing tuple", tN1, tN1d);
	}
	
	@Test
	public void testNullXML() throws Exception {
		Document d = db.newDocument();
		Element tuple = d.createElement("tuple");
		d.appendChild(tuple);
		
		tM.serialize(d, tuple);
		
		DomUtils.write(d, System.out);
		
		Tuple tMd = AbstractTuple.deserialize(tuple, s);
		assertEquals("Incorrect result from deserializing tuple", tM, tMd);
	}

	@Test
	public void testLabeledNullXML() throws Exception {
		Document d = db.newDocument();
		Element tuple = d.createElement("tuple");
		d.appendChild(tuple);
		
		tAl.serialize(d, tuple);
		
		DomUtils.write(d, System.out);
		
		Tuple tAld = AbstractTuple.deserialize(tuple, s);
		assertEquals("Incorrect result from deserializing tuple", tAl, tAld);
	}
}
