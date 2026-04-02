package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;



public class InitialConflict extends ConflictType {
	TxnPeerID initialTid;
	Tuple initialVal;
	
	public InitialConflict(TxnPeerID initialTid, Tuple initialVal) {
		this.initialTid = initialTid.duplicate();
		this.initialVal = initialVal.duplicate();
	}

	public ConflictTypeCode getTypeCode() {
		return ConflictTypeCode.INITIAL;
	}

	public boolean isApplicableFor(Update u) {
		return (initialTid.equals(u.getInitialTid()) && initialVal.equals(u.getInitialVal()));
	}

}
