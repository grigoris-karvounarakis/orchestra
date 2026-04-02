package edu.upenn.cis.orchestra.p2pqp;

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
import edu.upenn.cis.orchestra.datamodel.StringType;

public class TestFilters {
	QpSchema r, s;
	QpTuple<?> rN1, rN2, rM5, sN1, sM5;
	
	@Before
	public void createSchemaAndTuples() throws Exception {
		r = new QpSchema("R", 0);
		r.addCol("name", new StringType(true, false, true, 10));
		r.addCol("val", new IntType(false, false));
		r.markFinished();
		s = new QpSchema("S", 1);
		s.addCol("name", new StringType(true, false, true, 10));
		s.addCol("val", new IntType(false, false));
		s.markFinished();
		Object[] fields = new Object[2];
		fields[0] = "Nick";
		fields[1] = 1;
		rN1 = new QpTuple<Null>(r,fields);
		sN1 = new QpTuple<Null>(s,fields);
		fields[0] = "Nick";
		fields[1] = 2;
		rN2 = new QpTuple<Null>(r,fields);
		fields[0] = "Mark";
		fields[1] = 5;
		rM5 = new QpTuple<Null>(r,fields);
		sM5 = new QpTuple<Null>(s,fields);
	}
	
	@Test
	public void testBloomFilter() {
		int[] cols = {0,1};
		BloomFilter bf = new BloomFilter(100, 1, r, cols);
		assertEquals("Filter has wrong size", 127, bf.getNumBits());
		bf.add(rN1);
		
		assertTrue("Filter does not match rN1", bf.eval(rN1));
		assertFalse("Tuple matches rM5", bf.eval(rM5));
	}
	
	@Test
	public void testKeyBloomFilter() {
		BloomFilter bf = new BloomFilter(100, 1, r, new int[] {0});
		assertEquals("Filter has wrong size", 127, bf.getNumBits());
		bf.add(rN1);
		
		assertTrue("Filter does not match rN1", bf.eval(rN1));
		assertTrue("Filter does not match rN2", bf.eval(rN2));
		assertFalse("Tuple matches rM5", bf.eval(rM5));
		
	}
	
	
	@Test
	public void testByteifiedBloomFilter() throws Exception {
		BloomFilter bf = new BloomFilter(100, 1, r, new int[] {0});
		assertEquals("Filter has wrong size", 127, bf.getNumBits());
		bf.add(rN1);
		bf.changeRelation(s, new int[] {0});
		
		byte[] bytes = bf.getBytes();
		BloomFilter bf2 = new BloomFilter(s, bytes);
		assertTrue("Filter does not match sN1", bf2.eval(sN1));
		assertFalse("Filter matches sM5", bf2.eval(sM5));
	}
	
	@Test
	public void testSerializedBloomFilter() throws Exception {
		BloomFilter bf = new BloomFilter(100, 1, r, new int[] {0});
		assertEquals("Filter has wrong size", 127, bf.getNumBits());
		bf.add(rN1);
		bf.changeRelation(s, new int[] {0});

		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.newDocument();
		Element e = d.createElement("bloomFilter");
		d.appendChild(e);
		bf.serialize(d, e);
		
		BloomFilter bf2 = BloomFilter.deserialize(s,e);
		assertTrue("Filter does not match sN1", bf2.eval(sN1));
		assertFalse("Filter matches sM5", bf2.eval(sM5));
		
	}
}
