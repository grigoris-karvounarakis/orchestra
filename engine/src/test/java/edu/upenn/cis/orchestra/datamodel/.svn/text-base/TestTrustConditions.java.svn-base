package edu.upenn.cis.orchestra.datamodel;


import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.predicate.AndPred;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.Predicate;
import edu.upenn.cis.orchestra.predicate.ComparePredicate.Op;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.util.DomUtils;

public class TestTrustConditions {

	SchemaIDBinding scm;
	Schema s;
	Relation rs;
	Predicate atMost30, lessThan30, sameNameAndJob, am30AndSnaj;
	TrustConditions tc;
	AbstractPeerID owner;
	AbstractPeerID other1, other2, other3;
	List<Peer> peers = new ArrayList<Peer>();
	
	
	@Before
	public void setUp() throws Exception {
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
		ec.setTransactional(true);
		Environment e = new Environment(f, ec);
		
		scm = new SchemaIDBinding(e); 

		s = new Schema();
		rs = s.addRelation("R");
		rs.addCol("name", new StringType(true,true,true,10));
		rs.addCol("occupation", new StringType(true,true,true,10));
		rs.addCol("age", IntType.INT);
		s.markFinished();
		
		atMost30 = ComparePredicate.createColLit(rs, "age", Op.LE, 30);
		lessThan30 = ComparePredicate.createColLit(rs, "age", Op.LT, 30);
		sameNameAndJob = ComparePredicate.createTwoCols(rs, "name", Op.EQ, "occupation");
		am30AndSnaj = new AndPred(atMost30, sameNameAndJob);
		
		owner = new StringPeerID("me");
		other1 = new IntPeerID(512);
		other2 = new StringPeerID("you");
		other3 = new StringPeerID("them");
		peers.add(new Peer(owner.toString(), "localhost", ""));
		peers.add(new Peer(other1.toString(), "localhost", ""));
		peers.add(new Peer(other2.toString(), "localhost", ""));
		peers.add(new Peer(other3.toString(), "localhost", ""));
		for (Peer p : peers)
			p.addSchema(s);
		
		tc = new TrustConditions(owner);
		tc.addTrustCondition(other1, rs.getRelationID(), atMost30, 12);
		tc.addTrustCondition(other2, rs.getRelationID(), lessThan30, 17);
		tc.addTrustCondition(other1, rs.getRelationID(), sameNameAndJob, 19);
		tc.addTrustCondition(other2, rs.getRelationID(), am30AndSnaj, 3);
		tc.addTrustCondition(other3, rs.getRelationID(), null, 42);
	}

	@Test
	public void testByteification() throws Exception {
		byte[] tcb = tc.getBytes(scm);
		TrustConditions tcd = new TrustConditions(tcb,scm);
		assertEquals(tc,tcd);
	}
	
	@Test
	public void testXMLification() throws Exception {
		DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
		Document d = db.newDocument();
		Element root = d.createElement("trustConds");
		d.appendChild(root);
		
		tc.serialize(d, root, s);
		
		DomUtils.write(d, System.out);

		//TrustConditions tcd = TrustConditions.deserialize(root, s, owner);
		TrustConditions tcd = TrustConditions.deserialize(root, peers, owner);
		assertEquals(tc,tcd);

	}
}
