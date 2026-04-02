package edu.upenn.cis.orchestra.gui.peers;

import java.awt.Component;
import java.awt.Cursor;

import javax.swing.JOptionPane;
import javax.swing.SwingWorker;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.reconciliation.DbException;
import edu.upenn.cis.orchestra.reconciliation.StateStore.SSException;

public class PeerCommands {

	public static void reconcile (final Component parentComp, 
			final OrchestraSystem sys,
			final Peer p,
			final PeerTransactionsIntf peerTransIntf)
	{

		if ((peerTransIntf == null) || (peerTransIntf!=null && !peerTransIntf.hasCurrentTransaction()))
		{
			if (sys.getRecMode())
			{
				changeCursor(parentComp, true);
				new SwingWorker<Void, Void> ()
				{
					@Override
					protected Void doInBackground() throws SSException, DbException {
						sys.getRecDb(p.getId()).reconcile();
						return null;
					}

					@Override
					protected void done() {
						peerTransIntf.setRefreshDataViews(false);
						changeCursor(parentComp, false);
						try
						{
							get();
							JOptionPane.showMessageDialog(parentComp, "Reconciliation was successfull", "Reconciliation", JOptionPane.INFORMATION_MESSAGE);
						} catch (Exception ex)
						{
							ex.printStackTrace();
							JOptionPane.showMessageDialog(parentComp, "Error while reconciling: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);								
						}
					}
				}.execute();
			}
			else
			{
				changeCursor(parentComp, true);
				new SwingWorker<Void, Void> ()
				{
					@Override
					protected Void doInBackground() throws Exception {
						if (!sys.getMappingDb().isConnected())
							sys.getMappingDb().connect();

						// Now run the Exchange
						int lastrec = sys.getRecDb(p.getId()).getCurrentRecno();
						int recno = sys.getRecDb(p.getId()).getRecNo();
						sys.getMappingEngine().mapUpdates(lastrec, recno, p, false);
						sys.getRecDb(p.getId()).setRecDone();
						return null;
					}

					@Override
					protected void done() {
						if (peerTransIntf != null)
							peerTransIntf.setRefreshDataViews(false);
						changeCursor(parentComp, false);
						try
						{
							get();
							JOptionPane.showMessageDialog(parentComp, "Reconciliation was successful", "Reconciliation", JOptionPane.INFORMATION_MESSAGE);								
						} catch (Exception ex) {
							JOptionPane.showMessageDialog(parentComp, ex.getMessage(), "Error reconciling", JOptionPane.ERROR_MESSAGE);
							ex.printStackTrace();
						}
					}
				}.execute();					
			}
		}
		else
		{
			JOptionPane.showMessageDialog(parentComp, "You cannot reconcile unless you commit or rollback your changes first.", "Current transaction", JOptionPane.WARNING_MESSAGE);					
		}
	}

	public static void publish (Component parentComp,
			OrchestraSystem sys,
			Peer p, PeerTransactionsIntf peerTransIntf)
	{
		if (peerTransIntf!=null && !peerTransIntf.hasCurrentTransaction())
			try
		{
				sys.getMappingDb().fetchDbTransactions(p, sys.getRecDb(p.getId()));
				sys.getRecDb(p.getId()).publish();
				JOptionPane.showMessageDialog(parentComp, "Publication was successful", "Publication", JOptionPane.INFORMATION_MESSAGE);				
		} catch (SSException ex)
		{
			ex.printStackTrace();
			JOptionPane.showMessageDialog(parentComp, "Error while publishing: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
		} catch (Exception ex)
		{
			ex.printStackTrace();
			JOptionPane.showMessageDialog(parentComp, "Error while publishing: " + ex.getMessage(), "Error", JOptionPane.ERROR_MESSAGE);
		}
		else
			JOptionPane.showMessageDialog(parentComp, "You cannot publish unless you commit or rollback your changes first.", "Current transaction", JOptionPane.WARNING_MESSAGE);		
	}

	public static void changeCursor (Component parentComp, boolean isWait)
	{
		parentComp.setCursor(Cursor.getPredefinedCursor(isWait?Cursor.WAIT_CURSOR:Cursor.DEFAULT_CURSOR));
	}

}
