package edu.upenn.cis.orchestra.gui.peers;

import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.LayoutManager;

import javax.swing.BorderFactory;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.UIManager;

import edu.upenn.cis.orchestra.datamodel.Peer;

public class PeerInfoSidePanel extends JPanel {
	public static final long serialVersionUID = 1L;

	private final JTextArea _txtAreaPeerDesc = new JTextArea ();
	private final JLabel _labPeerId = new JLabel ();

	public PeerInfoSidePanel() {
		// TODO Auto-generated constructor stub
		init();
	}

	public PeerInfoSidePanel(LayoutManager arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
		init();
	}

	public PeerInfoSidePanel(boolean arg0) {
		super(arg0);
		// TODO Auto-generated constructor stub
		init();
	}

	public PeerInfoSidePanel(LayoutManager arg0, boolean arg1) {
		super(arg0, arg1);
		// TODO Auto-generated constructor stub
		init();
	}

	public void init() {
		// Create a first panel to show peer information
		setBorder(BorderFactory.createTitledBorder("Peer details"));
		setLayout(new GridBagLayout ());		

		GridBagConstraints cst = new GridBagConstraints ();
		cst.gridx = 0;
		cst.gridy = 0;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.weightx = 0.1; 
		_labPeerId.setFont(UIManager.getFont("PeerLabel.font"));//new Font("SERIF", Font.BOLD, 12));
		add (_labPeerId, cst);
		
		_txtAreaPeerDesc.setEditable(false);
		_txtAreaPeerDesc.setBackground(getBackground());
		_txtAreaPeerDesc.setLineWrap(true);
		_txtAreaPeerDesc.setWrapStyleWord(true);
		Font txtFont = UIManager.getFont("PeerDescription.font");//_txtAreaPeerDesc.getFont();
		//txtFont = txtFont.deriveFont((float)txtFont.getSize()-2F);
//		txtFont = new Font ("Serif", Font.PLAIN, 10);
		_txtAreaPeerDesc.setFont(txtFont);
		final JScrollPane scrollTxtDesc = new JScrollPane (_txtAreaPeerDesc);
		scrollTxtDesc.setBorder(BorderFactory.createEmptyBorder());
		cst.gridx = 0;
		cst.gridy = 1;
		cst.anchor = GridBagConstraints.FIRST_LINE_START;
		cst.fill = GridBagConstraints.BOTH;
		cst.weightx = 0.1;
		cst.weighty = 0.1; 
		add (scrollTxtDesc, cst);
		setVisible(false);
	}
	
	public void setPeer(Peer p) {
		_labPeerId.setText("Peer " + p.getId());
		_txtAreaPeerDesc.setText(p.getDescription());		
	}
}
