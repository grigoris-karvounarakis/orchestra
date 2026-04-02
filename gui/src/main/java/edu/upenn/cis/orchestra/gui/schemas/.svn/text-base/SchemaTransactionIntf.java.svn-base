package edu.upenn.cis.orchestra.gui.schemas;

import edu.upenn.cis.orchestra.datamodel.TxnPeerID;

public interface SchemaTransactionIntf {

	public boolean hasCurrentTransaction ();
	
	public TxnPeerID applyTransaction ()
				throws SchemaTransactionException;
	
	public void rollbackTransaction ();
	
}
