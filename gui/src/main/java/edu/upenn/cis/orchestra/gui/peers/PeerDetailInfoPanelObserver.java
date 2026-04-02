package edu.upenn.cis.orchestra.gui.peers;



import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

public interface PeerDetailInfoPanelObserver {

	public void mappingsBtnClicked (Peer p);
	
	public void schemaDetailsBtnClicked (Peer p, Schema s);
}
