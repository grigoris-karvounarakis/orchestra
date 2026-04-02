package edu.upenn.cis.orchestra.predicate;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.predicate.ComparePredicate.Op;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TestPredicates {

	Schema s;
	Relation rs;
	Tuple tNs20, tMd40, tTt30;
	Predicate atMost30, lessThan30, sameNameAndJob, am30AndSnaj;
	EqualityPredicate sameNameAndJobEP, age30, not30;
	
	
	@Before
	public void setUp() throws Exception {
		s = new Schema();
		rs = s.addRelation("R");
		rs.addCol("name", new StringType(true,true,true,10));
		rs.addCol("occupation", new StringType(true,true,true,10));
		rs.addCol("age", IntType.INT);
		s.markFinished();
		
		tNs20 = s.createTuple("R", "Nick", "student", 20);
		
		tMd40 = s.createTuple("R", "Mark", "doctor", 40);
		
		tTt30 = s.createTuple("R", "Tailor", "Tailor", 30);
		
		atMost30 = ComparePredicate.createColLit(rs, "age", Op.LE, 30);
		lessThan30 = ComparePredicate.createColLit(rs, "age", Op.LT, 30);
		sameNameAndJob = ComparePredicate.createTwoCols(rs, "name", Op.EQ, "occupation");
		am30AndSnaj = new AndPred(atMost30, sameNameAndJob);
		age30 = new EqualityPredicate(2, 30, rs, false);
		not30 = new EqualityPredicate(2, 30, rs, true);
		sameNameAndJobEP = new EqualityPredicate(rs, 0, 1, false);
	}
	
	@Test
	public void testEvaluation() throws Exception {
		assertTrue(atMost30.eval(tNs20));
		assertTrue(atMost30.eval(tTt30));
		assertFalse(atMost30.eval(tMd40));
		
		assertTrue(lessThan30.eval(tNs20));
		assertFalse(lessThan30.eval(tTt30));
		assertFalse(lessThan30.eval(tMd40));
		
		assertTrue(sameNameAndJob.eval(tTt30));
		assertFalse(sameNameAndJob.eval(tNs20));
		assertFalse(sameNameAndJob.eval(tMd40));

		assertTrue(am30AndSnaj.eval(tTt30));
		assertFalse(am30AndSnaj.eval(tNs20));
		assertFalse(am30AndSnaj.eval(tMd40));
		
		assertTrue(age30.eval(tTt30));
		assertFalse(age30.eval(tNs20));
		assertFalse(age30.eval(tMd40));
		
		assertFalse(not30.eval(tTt30));
		assertTrue(not30.eval(tNs20));
		assertTrue(not30.eval(tMd40));

		assertTrue(sameNameAndJobEP.eval(tTt30));
		assertFalse(sameNameAndJobEP.eval(tNs20));
		assertFalse(sameNameAndJobEP.eval(tMd40));
}
	
	@Test
	public void testByteification() throws Exception {
		byte[] atMost30b = Byteification.getPredicateBytes(rs, atMost30);
		byte[] lessThan30b = Byteification.getPredicateBytes(rs, lessThan30);
		byte[] sameNameAndJobb = Byteification.getPredicateBytes(rs, sameNameAndJob);
		byte[] am30AndSnajb = Byteification.getPredicateBytes(rs, am30AndSnaj);
		
		Predicate atMost30d = Byteification.getPredicateFromBytes(rs, atMost30b);
		Predicate lessThan30d = Byteification.getPredicateFromBytes(rs, lessThan30b);
		Predicate sameNameAndJobd = Byteification.getPredicateFromBytes(rs, sameNameAndJobb);
		Predicate am30AndSnajd = Byteification.getPredicateFromBytes(rs, am30AndSnajb);

		assertEquals(atMost30, atMost30d);
		assertEquals(lessThan30, lessThan30d);
		assertEquals(sameNameAndJob, sameNameAndJobd);
		assertEquals(am30AndSnaj, am30AndSnajd);
	}
	
	@Test
	public void testXMLification() throws Exception {
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.newDocument();
		Element root = d.createElement("root");
		d.appendChild(root);
		
		Element am30e = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(atMost30, d, am30e, rs);
		Element lt30e = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(lessThan30, d, lt30e, rs);
		Element snje = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(sameNameAndJob, d, snje, rs);
		Element am30AndSnaje = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(am30AndSnaj, d, am30AndSnaje, rs);
		Element snajEPe = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(sameNameAndJobEP, d, snajEPe, rs);
		Element age30e = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(age30, d, age30e, rs);
		Element not30e = DomUtils.addChild(d, root, "pred");
		XMLification.serialize(not30, d, not30e, rs);

		
		root.appendChild(am30e);
		root.appendChild(lt30e);
		root.appendChild(snje);
		root.appendChild(am30AndSnaje);
		root.appendChild(snajEPe);
		root.appendChild(age30e);
		root.appendChild(not30e);
		
		DomUtils.write(d, System.out);

		Predicate atMost30d = XMLification.deserialize(am30e, rs);
		Predicate lessThan30d = XMLification.deserialize(lt30e, rs);
		Predicate sameNameAndJobd = XMLification.deserialize(snje, rs);
		Predicate am30AndSnajd = XMLification.deserialize(am30AndSnaje, rs);
		Predicate sameNameAndJobEPd = XMLification.deserialize(snajEPe, rs);
		Predicate age30d = XMLification.deserialize(age30e, rs);
		Predicate not30d = XMLification.deserialize(not30e, rs);
		
		
		assertEquals(atMost30, atMost30d);
		assertEquals(lessThan30, lessThan30d);
		assertEquals(sameNameAndJob, sameNameAndJobd);
		assertEquals(am30AndSnaj, am30AndSnajd);
		assertEquals(sameNameAndJobEP, sameNameAndJobEPd);
		assertEquals(age30, age30d);
		assertEquals(not30, not30d);
	}

}
