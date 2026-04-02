package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

class GetTxns implements Serializable {
	private static final long serialVersionUID = 1L;
	private final byte tpis[];
	
	GetTxns(Collection<TxnPeerID> tpis) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		for (TxnPeerID tpi : tpis) {
			bbw.addToBuffer(tpi);
		}
		this.tpis = bbw.getByteArray();
	}
	
	List<TxnPeerID> getTpis() {
		ArrayList<TxnPeerID> retval = new ArrayList<TxnPeerID>();
		
		ByteBufferReader bbr = new ByteBufferReader(tpis);
		while (! bbr.hasFinished()) {
			retval.add(bbr.readTxnPeerID());
		}
		
		return retval;
	}
}
