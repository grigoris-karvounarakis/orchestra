/**
 * 
 */
package edu.upenn.cis.orchestra.reconciliation.bdbstore;

import java.io.Serializable;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

class GetReconciliationData implements Serializable {
	private static final long serialVersionUID = 1L;
	int recno;
	Set<TxnPeerID> ownAcceptedTxns;
	GetReconciliationData(int recno, Set<TxnPeerID> ownAcceptedTxns) {
		this.recno = recno;
		this.ownAcceptedTxns = ownAcceptedTxns;
	}
}