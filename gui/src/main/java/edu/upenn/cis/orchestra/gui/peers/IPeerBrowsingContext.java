/**
 * 
 */
package edu.upenn.cis.orchestra.gui.peers;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

/**
 * Interface for peer context when browsing
 * 
 * @author zives
 *
 */
public interface IPeerBrowsingContext {
	public Peer getCurrentPeer();
	public PeerTransactionsIntf getPeerTransactionsIntf ();
	public Schema getCurrentSchema();
	
	public void setCurrentSchema(Schema s);
	public void setCurrentPeer(Peer p, PeerTransactionsIntf peerTrans);
	
	public enum BrowseState {PEER_VIEW, MAPPING_VIEW, SCHEMA_VIEW, TRANS_VIEW, PROV_VIEW};
	
	public BrowseState getBrowseState();
	public void setBrowseState(BrowseState b);
	
	public void setRefreshTransactions();
	
	/**
	 * Should return the index of a pane within a given view: if there are multiple
	 * peer views, etc., it should be the 0th, 1st, etc.
	 * 
	 * @return
	 */
	public int getIndex();
	public void setIndex(int index);
}
