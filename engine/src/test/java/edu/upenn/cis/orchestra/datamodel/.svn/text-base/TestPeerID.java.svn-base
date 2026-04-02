package edu.upenn.cis.orchestra.datamodel;


import static org.junit.Assert.assertEquals;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import edu.upenn.cis.orchestra.util.DomUtils;

public class TestPeerID {

	AbstractPeerID spi;
	AbstractPeerID ipi;
	
	@Before
	public void setUp() throws Exception {
		ipi = new IntPeerID(777);
		spi = new StringPeerID("¡Non più andrai, farfallone amaroso!");
	}

	@Test
	public void testXML() throws Exception {
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.newDocument();
		Element root = d.createElement("root");
		d.appendChild(root);
		Element p1 = d.createElement("peer");
		root.appendChild(p1);
		Element p2 = d.createElement("peer");
		root.appendChild(p2);
		spi.serialize(d, p1);
		ipi.serialize(d, p2);
		DomUtils.write(d, System.out);

		AbstractPeerID spid = AbstractPeerID.deserialize(p1);
		AbstractPeerID ipid = AbstractPeerID.deserialize(p2);
		
		assertEquals("Error decoding StringPeerID", spi, spid);
		assertEquals("Error decoding IntPeerID", ipi, ipid);
	}
	
	@Test
	public void testString() throws Exception {
		String ipis = ipi.serialize();
		AbstractPeerID ipid = AbstractPeerID.deserialize(ipis);
		assertEquals("IntPeerID deserialization failed", ipi, ipid);
		
		String spis = spi.serialize();
		AbstractPeerID spid = AbstractPeerID.deserialize(spis);
		assertEquals("StringPeerID deserialization failed", spi, spid);		
	}
}
