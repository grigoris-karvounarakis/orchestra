package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;

import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;

class GetSchema implements Serializable {
	private static final long serialVersionUID = 1L;
	private final byte[] pid;
	
	GetSchema(AbstractPeerID pid) {
		this.pid = pid.getBytes();
	}
	
	AbstractPeerID getPid() {
		return AbstractPeerID.fromBytes(pid);
	}
	
}
