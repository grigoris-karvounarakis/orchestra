package edu.upenn.cis.orchestra.gui.transactions;

import javax.swing.JInternalFrame;
import javax.swing.JOptionPane;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.reconciliation.DbException;

public class TransactionIFrame extends JInternalFrame {
	public static final long serialVersionUID = 1L;
	TransactionViewer _transView;

	public TransactionIFrame (Peer p, OrchestraSystem sys)
	{
		super ("Transaction view: " + p.getId(), true, true, true, true);
		try {
			_transView = new TransactionViewer(p, sys);
			add (_transView);
			//_transView.setVisible(true);
		} catch (DbException e) {
			JOptionPane.showMessageDialog(this, e.getMessage(), "Error accessing DB", JOptionPane.ERROR_MESSAGE);
			e.printStackTrace();
		} 
	}
	
	public void close() {
		/*
		try {
			_transView.tearDown();
		} catch (Exception e) {
			e.printStackTrace();
		}*/
	}
}
