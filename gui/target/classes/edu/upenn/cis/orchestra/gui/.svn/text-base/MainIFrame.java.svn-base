package edu.upenn.cis.orchestra.gui;

import java.awt.BorderLayout;
import java.io.File;

import javax.swing.JInternalFrame;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class MainIFrame extends JInternalFrame {
	public static final long serialVersionUID = 1L;
	
	private MainPanel _panel;
	private String _cdssname;
	private String _filename;

	public MainIFrame (GUIObjectsFactory objFactory, String cdssName, String filename)
	{
		super (cdssName + " CDSS - Peer View", true, false, true, true);
		
		setLayout (new BorderLayout ());
		String path;
		try {
			path = new File(filename).getParent();
		} catch (Exception e) {
			path = Config.getWorkDir();
		}
		_panel = new MainPanel(objFactory.getRepositoryDAO().loadAllPeers(), path);
		add (_panel, BorderLayout.CENTER);
		_cdssname = cdssName;
		_filename = filename;
	}

	public MainIFrame (OrchestraSystem catalog, String cdssName, String filename)
	{
		super (cdssName + " CDSS - Peer View", true, false, true, true);
		
		setLayout (new BorderLayout ());
		String path;
		try {
			path = new File(filename).getParent();
		} catch (Exception e) {
			path = Config.getWorkDir();
		}
		_panel = new MainPanel(catalog, path);
		add (_panel, BorderLayout.CENTER);
		_cdssname = cdssName;
		_filename = filename;
	}
	
	public String getFilename()
	{
		return _filename;
	}

	public String getCDSSName()
	{
		return _cdssname;
	}


	public MainPanel getPanel ()
	{
		return _panel;
	}
	
}
