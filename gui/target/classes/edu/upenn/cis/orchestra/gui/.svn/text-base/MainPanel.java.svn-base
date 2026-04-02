package edu.upenn.cis.orchestra.gui;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.io.File;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JPanel;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.AbstractPeerID;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.peers.PeersMgtPanel;
import edu.upenn.cis.orchestra.gui.provenance.ProvenanceViewer;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;
import edu.upenn.cis.orchestra.reconciliation.bdbstore.BerkeleyDBStoreServer;

public class MainPanel extends JPanel 
{
	public static final long serialVersionUID = 1L;
	
	
	private final CardLayout _cardLayout = new CardLayout ();
	private final String CARD_LAYOUT_PEERSMGT_INDEX = "PeersMgt";
	private final String CARD_LAYOUT_PROVVIEWER_INDEX = "ProvViewer";
	
	private static final String storeName = "updateStore";
	
	private final PeersMgtPanel _peersMgtPanel;
	
	private final JPanel _rightPanel;
	
	private final OrchestraSystem _system;
	
	private ProvenanceViewer _provViewer = null;
	
	private BerkeleyDBStoreServer _storeServer = null;
	
	private Environment _env = null;
	
	public MainPanel(OrchestraSystem system, String catalogPath)
	{
		super (new BorderLayout ());
		
		_system = system;
		_peersMgtPanel = new PeersMgtPanel (system, catalogPath);
		
		_rightPanel = new JPanel (_cardLayout);
		_rightPanel.add(_peersMgtPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		
		add (_rightPanel, BorderLayout.CENTER);
		
	}
	
	public OrchestraSystem getSystem()
	{
		return _system;
	}
	
	protected PeersMgtPanel getPeersMgtPanel ()
	{
		return _peersMgtPanel;
	}
	
	public void showPeerInformation(Peer p) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerInfo(p);		
	}
	
	public void showPeerSchema(Peer p, Schema s) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerSchema(p, s);
	}
	
	public void showPeersNetwork() {
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerNetwork();		
	}
	
	public void showPeerMappings(Peer p) {
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showPeerMappings(p);
	}
	
	public void showConsole() 
	{
		_peersMgtPanel.showConsole();
	}
	
	private void initProvenanceViewer ()
	{
		if (_provViewer == null)
		{
			_provViewer = new ProvenanceViewer (_system, new RelationDataEditorFactory(_system));
			_rightPanel.add(_provViewer, CARD_LAYOUT_PROVVIEWER_INDEX);
		}		
	}
	
	public void showProvenanceViewer() 
	{
		initProvenanceViewer();
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PROVVIEWER_INDEX);
	}
	
	public void showProvenanceViewer(Peer p, Schema s) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showProvenanceViewer(p, s);
	}

	public void showProvenanceViewer(Peer p) 
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showProvenanceViewer(p);
	}
	
	public void showTransactionViewer(Peer p)
	{
		_cardLayout.show(_rightPanel, CARD_LAYOUT_PEERSMGT_INDEX);
		_peersMgtPanel.showTransactionViewerTab(p);		
	}
	
	public void startStoreServer() throws Exception {
		InetSocketAddress ias = _system.getBdbStorePort();
		if (ias == null) {
			throw new Exception("System does not use a BerkeleyDB Store server");
		}
		// TODO: check that the server should actually be running on
		// the local machine
		if (!_system.isLocalUpdateStore())
			return;
		
		if (_storeServer == null) {
//			File f = new File(_system.getName() + "_env");
			File f = new File(storeName + "_env");
			if (! f.exists()) {
				f.mkdir();
			}
	
//			File configFile = new File(_system.getName() + ".config");
			
			EnvironmentConfig ec = new EnvironmentConfig();
			ec.setAllowCreate(true);
			ec.setTransactional(true);
			_env = new Environment(f, ec);
			_storeServer = new BerkeleyDBStoreServer(_env, ias.getPort());
		}
		Map<AbstractPeerID,Integer> peerSchemas = new HashMap<AbstractPeerID,Integer>();
		for (Peer p: _system.getPeers())
			peerSchemas.put(p.getPeerId(), _system.getAllSchemas().indexOf(p.getSchemas().iterator().next()));
			
		_storeServer.registerAllSchemas(_system.getName(), _system.getAllSchemas(), peerSchemas);
		
		_system.setSchemaIDBinding(_storeServer.getBinding());
		return;
//		throw new Exception("Server is alreadying running");
//	}
	}
	
	public void clearStoreServer() throws IllegalStateException {
		if (storeServerRunning()) {
			throw new IllegalStateException("Cannot clear store server while store server is running");
		}
		File f = new File(storeName + "_env");
		if (f.exists()) {
				File[] files = f.listFiles();
				for (File file : files) {
					file.delete();
				}
		} else {
			f.mkdir();
		}
		/*
		File configFile = new File(_system.getName() + ".config");
		if (configFile.exists()) {
			configFile.delete();
		}*/
	}
	
	public boolean storeServerRunning() {
		return (_storeServer != null);
	}
	
	public void stopStoreServer() throws Exception {
		if (_storeServer != null) {
			_storeServer.quit();
			_storeServer = null;
//			_env.close();
		}
	}
}
