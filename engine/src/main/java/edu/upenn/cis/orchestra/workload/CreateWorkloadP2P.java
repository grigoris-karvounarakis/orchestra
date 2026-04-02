package edu.upenn.cis.orchestra.workload;

import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.datamodel.IntPeerID;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.reconciliation.BerkeleyDBStore;
import edu.upenn.cis.orchestra.reconciliation.ClientCentricDb;
import edu.upenn.cis.orchestra.reconciliation.Db;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.StateStore;
import edu.upenn.cis.orchestra.reconciliation.p2pstore.PastryStoreFactory;

public class CreateWorkloadP2P {
	public static void main(String[] args) throws Exception {
		CreateWorkload cw = new CreateWorkload();
		PastryFactory pf = new PastryFactory();
		cw.createWorkload(pf, args);
		pf.usf.shutdownEnvironment();
	}

	static class PastryFactory implements CreateWorkload.DatabaseFactory {
		PastryStoreFactory usf = null;
		public Db createDb(int peerID, Schema s, Environment env) throws Exception {
			if (usf == null) {
				usf = new PastryStoreFactory(6000, env, 5, "p2pstore_epoch", "p2pstore_txn",
				"p2pstore_recon", "p2pstore_decision", "p2pstore_pastryids");
			}
			StateStore.Factory ssf = new BerkeleyDBStore.Factory(env, "statestore_state" + peerID, "statestore_updates" + peerID);
			SchemaIDBinding scm = new SchemaIDBinding(env); 
			return new ClientCentricDb(new OrchestraSystem(scm), scm, s, new IntPeerID(peerID), usf, ssf);
		}

		public void initDb(Schema s) throws Exception {
		}
		
	}
}
