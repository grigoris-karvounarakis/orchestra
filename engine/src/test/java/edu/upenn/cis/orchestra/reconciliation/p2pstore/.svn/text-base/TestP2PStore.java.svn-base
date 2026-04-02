package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.StringType;
import edu.upenn.cis.orchestra.datamodel.TrustConditions;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.predicate.ComparePredicate;
import edu.upenn.cis.orchestra.predicate.ComparePredicate.Op;
import edu.upenn.cis.orchestra.reconciliation.Decision;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.TxnChain;


public class TestP2PStore {
	Environment e;
	AbstractPeerID pid1, pid2;
	P2PStore p1, p2;
	TrustConditions tc1, tc2;
	Schema schema;
	SchemaIDBinding schMap;
	Relation rs;
	static final int port = 6000;
	
	Tuple tJ1;
	Tuple tN2;
	Tuple tM3;
	Update insJ1;
	Update modJ1N2;
	Update insM3;
	
	PastryStoreFactory psf;
	
	@Before
	public void setUp() throws Exception {
		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				if (! file.delete()) {
					System.err.println("Couldn't delete " + file.getName() + File.separator + file);
				}
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		e = new Environment(f, ec);
		
		schMap = new SchemaIDBinding(e); 

		schema = new Schema();
		rs = schema.addRelation("R");
		rs.addCol("name", new StringType(true, false, true, 10));
		rs.addCol("val", new IntType(false,false));
		schema.markFinished();
		
		pid1 = new IntPeerID(1);
		pid2 = new IntPeerID(2);

		tJ1 = new Tuple(rs);
		tJ1.set("name", "James");
		tJ1.set("val", 1);
		tN2 = new Tuple(rs);
		tN2.set("name", "Nick");
		tN2.set("val", 2);
		tM3 = new Tuple(rs);
		tM3.set("name", "Mark");
		tM3.set("val", 3);
		insJ1 = new Update(null, tJ1);
		insJ1.addTid(1, pid2);
		modJ1N2 = new Update(tJ1, tN2);
		modJ1N2.addTid(2, pid2);
		modJ1N2.addPrevTid(insJ1.getLastTid());
		insM3 = new Update(null, tM3);
		insM3.addTid(1, pid1);
		
		
		psf = new PastryStoreFactory(port, e, 2, "epoch", "transaction", "reconciliation", "acceptedRejected", "pastryIds");
		
		
		tc1 = new TrustConditions(pid1);
		tc2 = new TrustConditions(pid2);
		tc1.addTrustCondition(pid2, rs.getRelationID(), ComparePredicate.createColLit(rs, "name", Op.EQ, "Nick"), 3);
		tc2.addTrustCondition(pid1, rs.getRelationID(), ComparePredicate.createColLit(rs, "name", Op.NE, "Nick"), 3);
		
		p1 = psf.getUpdateStore(pid1, schMap, schema, tc1);
		p2 = psf.getUpdateStore(pid2, schMap, schema, tc2);
	}
	

	@Test
	public void testPublishAndRetrieve() throws Exception {
		ArrayList<Update> txn = new ArrayList<Update>();
		txn.add(insJ1);
		ArrayList<List<Update>> txns = new ArrayList<List<Update>>();
		txns.add(txn);
		ArrayList<Decision> accepted = new ArrayList<Decision>();
		accepted.add(new Decision(insJ1.getLastTid(), PeerController.FIRST_RECNO, true));
		
		// P2 reconciliation 0
		p2.publish(txns);
		p2.recordReconcile(false);
		p2.recordTxnDecisions(accepted);

		txn = new ArrayList<Update>();
		txn.add(modJ1N2);
		txns = new ArrayList<List<Update>>();
		txns.add(txn);		
		accepted.clear();
		accepted.add(new Decision(modJ1N2.getLastTid(), PeerController.FIRST_RECNO + 1, true));
		
		// P2 reconciliation 1
		p2.publish(txns);
		p2.recordReconcile(false);
		p2.recordTxnDecisions(accepted);
		
		txn = new ArrayList<Update>();
		txn.add(insM3);
		txns = new ArrayList<List<Update>>();
		txns.add(txn);
		
		p1.publish(txns);
		
		// P1 reconciliation 0
		p1.recordReconcile(false);

		// P2 reconciliation 2
		p2.recordReconcile(false);

		HashMap<Integer,List<TxnChain>> p1Txns = new HashMap<Integer,List<TxnChain>>();
		HashMap<Integer,List<TxnChain>> p2Txns = new HashMap<Integer,List<TxnChain>>();
		HashSet<TxnPeerID> p1MustReject = new HashSet<TxnPeerID>();
		HashSet<TxnPeerID> p2MustReject = new HashSet<TxnPeerID>();
		
		Set<TxnPeerID> noTids = Collections.emptySet();
		
		p1.getReconciliationData(p1.getCurrentRecno(), Collections.singleton(insM3.getLastTid()), p1Txns, p1MustReject);
		p2.getReconciliationData(p2.getCurrentRecno(), noTids, p2Txns, p2MustReject);
		assertNotNull("Should have transactions of priority 3", p1Txns.get(3));
		assertNotNull("Should have transactions of priority 3", p2Txns.get(3));
		assertEquals(1, p1Txns.get(3).size());
		assertEquals(1, p2Txns.get(3).size());

		TxnChain p1Chain = p1Txns.get(3).get(0);
		TxnChain p2Chain = p2Txns.get(3).get(0);

		assertEquals(modJ1N2.getLastTid(), p1Chain.getHead());
		assertEquals(2, p1Chain.getContents().size());
		assertEquals(insJ1, p1Chain.getContents().get(0));
		assertEquals(modJ1N2, p1Chain.getContents().get(1));
		
		assertEquals(insM3.getLastTid(), p2Chain.getHead());
		assertEquals(1,p2Chain.getContents().size());
		assertEquals(insM3, p2Chain.getContents().get(0));

	}
		
	@After
	public void tearDown() throws Exception {
		p1.disconnect();
		p2.disconnect();
		Thread.sleep(1000);
		psf.shutdownEnvironment();
		e.close();
	}
	
}
