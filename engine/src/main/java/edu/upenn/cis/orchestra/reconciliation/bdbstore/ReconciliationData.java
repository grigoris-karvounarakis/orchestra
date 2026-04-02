/**
 * 
 */
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;

import edu.upenn.cis.orchestra.datamodel.ByteBufferReader;
import edu.upenn.cis.orchestra.datamodel.ByteBufferWriter;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.SchemaIDBinding;
import edu.upenn.cis.orchestra.reconciliation.TxnChain;

class ReconciliationData implements Serializable {
	private static final long serialVersionUID = 1L;
	
	transient ByteBufferWriter bbw;
	transient ByteBufferReader bbr;
//	transient Schema s;
	transient SchemaIDBinding _map;
	private byte[] data;
	
	ReconciliationData() {
		bbw = new ByteBufferWriter();
	}
	
	void finish() {
		data = bbw.getByteArray();
		bbw = null;
	}
	
	void writeEntry(TxnPeerID tpi, int prio, TxnChain tc) {
		bbw.addToBuffer(tpi);
		bbw.addToBuffer(prio);
		if (tc == null) {
			bbw.addToBuffer((byte[]) null);
		} else {
			bbw.addToBuffer(tc.getBytes(true));
		}
	}
	
	static class Entry {
		TxnPeerID tpi;
		int prio;
		TxnChain tc;
		Entry(TxnPeerID tpi, int prio, TxnChain tc) {
			this.tpi = tpi;
			this.prio = prio;
			this.tc = tc;
		}
	}
	
	void beginReading(SchemaIDBinding s) {
		this._map = s;
		bbr = new ByteBufferReader(s, data);
//		this.s = s;
	}
	
	ReconciliationData.Entry readEntry() {
		if (bbr.hasFinished()) {
			return null;
		}
		
		TxnPeerID tpi = bbr.readTxnPeerID();
		int prio = bbr.readInt();
		byte[] chain = bbr.readByteArray();
		TxnChain tc;
		if (chain == null || chain.length == 0) {
			tc = null;
		} else {
//			tc = TxnChain.fromBytes(s, chain, 0, chain.length);
			tc = TxnChain.fromBytes(_map, chain, 0, chain.length);
		}
		return new Entry(tpi, prio, tc);
	}
	
}