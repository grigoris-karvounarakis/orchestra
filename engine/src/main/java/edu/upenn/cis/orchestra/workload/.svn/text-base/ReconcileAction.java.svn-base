package edu.upenn.cis.orchestra.workload;

import edu.upenn.cis.orchestra.reconciliation.Db;

import java.util.Map;

public class ReconcileAction extends WorkloadAction {
	private static final long serialVersionUID = 1L;

	public ReconcileAction(int peer) {
		super(peer);
	}

	protected void doAction(Map<Integer,Db> dbs, LockManagerClient lmc) throws Exception {
		if (lmc != null) {
			lmc.getLock(peer);
		}
		dbs.get(peer).publish();
		dbs.get(peer).reconcile();
		if (lmc != null) {
			lmc.releaseLock();
		}
	}

	public String toString() {
		return "Peer " + peer + ": reconcile";
	}

}
