/**
 * 
 */
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;
import java.io.IOException;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

class GetStatusMsg implements Serializable {
	private static final long serialVersionUID = 1L;
	TxnPeerID tpi;
	
	GetStatusMsg(TxnPeerID tpi) {
		this.tpi = tpi;
	}

	private void writeObject(java.io.ObjectOutputStream out) throws IOException {
		byte[] tpiBytes = tpi.getBytes();
		out.writeInt(tpiBytes.length);
		out.write(tpiBytes);
	}

	private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
		int tpiLength = in.readInt();
		byte[] tpiBytes = new byte[tpiLength];
		in.read(tpiBytes);
		tpi = TxnPeerID.fromBytes(tpiBytes, 0, tpiBytes.length);
	}
}