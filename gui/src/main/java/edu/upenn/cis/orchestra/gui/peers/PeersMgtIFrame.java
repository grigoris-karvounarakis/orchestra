package edu.upenn.cis.orchestra.gui.peers;

import java.awt.BorderLayout;

import javax.swing.JInternalFrame;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class PeersMgtIFrame extends JInternalFrame {
	public static final long serialVersionUID = 1L;
	
	private PeersMgtPanel _panel;
	
	public PeersMgtIFrame (OrchestraSystem peers, String cdssName, String catalogPath)
	{
		super (cdssName + " CDSS Management", true, false, true, true);
		
		setLayout (new BorderLayout ());
		_panel = new PeersMgtPanel (peers, catalogPath);
		add (_panel, BorderLayout.CENTER);
	}
	
	public PeersMgtPanel getPanel ()
	{
		return _panel;
	}
	
}
