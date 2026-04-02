package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;

class PublishMsg implements Serializable {
	private static final long serialVersionUID = 1L;
	private byte[] txns;
	
	transient ByteBufferReader txnsReader;
	
	PublishMsg(List<List<Update>> txns) {
		ByteBufferWriter bbw = new ByteBufferWriter();
		ByteBufferWriter bbw2 = new ByteBufferWriter();
		for (List<Update> txn : txns) {
			bbw2.clear();
			for (Update u : txn) {
				bbw2.addToBuffer(u, Update.SerializationLevel.VALUES_AND_TIDS);
			}
			bbw.addToBuffer(txn.get(0).getLastTid());
			bbw.addToBuffer(bbw2.getByteArray());
		}
		this.txns = bbw.getByteArray();
	}
	
	void startReading() {
		txnsReader = new ByteBufferReader(null, txns);
	}
	
	static class TxnToPublish {
		TxnPeerID tpi;
		byte[] contents;
	}
	
	TxnToPublish readTxn() {
		if (txnsReader.hasFinished()) {
			return null;
		}
		
		TxnToPublish ttp = new TxnToPublish();
		ttp.tpi = txnsReader.readTxnPeerID();
		ttp.contents = txnsReader.readByteArray();
		
		return ttp;
	}	
}
