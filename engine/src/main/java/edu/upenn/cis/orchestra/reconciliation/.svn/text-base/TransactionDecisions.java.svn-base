package edu.upenn.cis.orchestra.reconciliation;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.reconciliation.UpdateStore.USException;

public interface TransactionDecisions {
	/**
	 * Determine if the viewing peer has accepted a transaction
	 * 
	 * @param txn			The ID of the transaction and of the peer that inserted it
	 * @return 				<code>true</code> if the viewing peer has accepted it,
	 * 						<code>false</code> otherwise.
	 * @throws Exception
	 */
	boolean hasRejectedTxn(TxnPeerID tpi) throws USException;

	/**
	 * Determine if the viewing peer has rejected a transaction.
	 * 
	 * @param txn			The ID of the transaction and of the peer that inserted it
	 * @return 				<code>true</code> if the viewing peer has accepted it,
	 * 						<code>false</code> otherwise.
	 * @throws Exception
	 */
	boolean hasAcceptedTxn(TxnPeerID tpi) throws USException;
	
}