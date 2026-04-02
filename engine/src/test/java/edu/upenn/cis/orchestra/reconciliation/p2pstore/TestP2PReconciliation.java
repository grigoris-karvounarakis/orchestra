package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.io.File;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.TestReconciliation;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.Factory;

public class TestP2PReconciliation extends TestReconciliation {
	PastryStoreFactory factory;
	Environment e;
	
	int port = 6000;
	
	public void setUp() throws Exception {
		File f = new File("dbenv");
		if (f.exists()) {
			File[] files = f.listFiles();
			for (File file : files) {
				if ( ! file.delete()) {
					throw new Exception("Could not delete " + f.getName() + File.separator + file.getName());
				}
			}
		} else {
			f.mkdir();
		}
		EnvironmentConfig ec = new EnvironmentConfig();
		ec.setAllowCreate(true);
		ec.setTransactional(true);
		e = new Environment(f, ec);
		factory = new PastryStoreFactory(port, e, 3, "epoch", "transaction", "reconciliation", "acceptedRejected", "pastryIds");
		super.setUp();
	}
	
	public void tearDown() throws Exception {
		for (P2PStore store : factory.stores) {
			System.out.println(store.getPeerID() + ": " + store.replicationController.getLocalData());
		}
		super.tearDown();
		Thread.sleep(100);
		factory.shutdownEnvironment();
		e.close();
		factory = null;
		port += numPeers;
	}
	
	@Override
	protected Factory getStoreFactory() {
		return factory;
	}

	@Override
	protected void clearState(Schema s) throws Exception {
	}

}
