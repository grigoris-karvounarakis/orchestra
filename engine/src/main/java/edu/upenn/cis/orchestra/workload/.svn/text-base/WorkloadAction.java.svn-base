package edu.upenn.cis.orchestra.workload;

import java.util.Map;

import edu.upenn.cis.orchestra.reconciliation.Db;

public abstract class WorkloadAction implements java.io.Serializable {
	int peer;
	WorkloadAction(int peer) {
		this.peer = peer;
	}

	abstract void doAction(Map<Integer,Db> dbs, LockManagerClient lmc) throws Exception;

	public abstract String toString();
	
	public int getPeer() {
		return peer;
	}
}
