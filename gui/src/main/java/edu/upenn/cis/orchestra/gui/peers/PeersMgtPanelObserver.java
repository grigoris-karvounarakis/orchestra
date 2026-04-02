package edu.upenn.cis.orchestra.gui.peers;

import javax.swing.JComponent;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.datamodel.Schema;

public interface PeersMgtPanelObserver 
{
	public abstract void peerWasSelected(PeersMgtPanel panel, Peer p, Schema s, PeerTransactionsIntf peerTransIntf);
	public abstract void peerContextMenu(PeersMgtPanel panel, Peer p, Schema s, PeerTransactionsIntf peerTransIntf, JComponent parent, int x, int y);
	public abstract void mappingWasSelected(PeersMgtPanel panel, Mapping m);
	public abstract void selectionIsEmpty(PeersMgtPanel panel);
}
