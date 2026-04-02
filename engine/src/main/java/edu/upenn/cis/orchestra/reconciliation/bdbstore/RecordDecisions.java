package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.Decision;

class RecordDecisions implements Serializable {
	private static final long serialVersionUID = 1L;
	private byte[] decisions;
	
	transient ByteBufferReader bbr;
	
	RecordDecisions(Iterable<Decision> decisions) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (Decision decision : decisions) {
			bbw.addToBuffer(decision.tpi);
			bbw.addToBuffer(decision.recno);
			bbw.addToBuffer(decision.accepted);
		}
		this.decisions = bbw.getByteArray();
	}

	void startReading() {
		bbr = new ByteBufferReader(null, decisions);
	}
	
	Decision readDecision() {
		if (bbr.hasFinished()) {
			return null;
		}
		TxnPeerID tpi = bbr.readTxnPeerID();
		int recno = bbr.readInt();
		boolean accept = bbr.readBoolean();
		
		return new Decision(tpi, recno, accept);
	}
}