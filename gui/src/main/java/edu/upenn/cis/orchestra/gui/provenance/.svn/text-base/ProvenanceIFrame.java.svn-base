package edu.upenn.cis.orchestra.gui.provenance;

import javax.swing.JInternalFrame;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.gui.schemas.RelationDataEditorFactory;

public class ProvenanceIFrame extends JInternalFrame {
	public static final long serialVersionUID = 1L;
	ProvenanceViewer _provView;

	public ProvenanceIFrame (Peer p, Schema s, OrchestraSystem sys, RelationDataEditorFactory dataEditFactory)
	{
		super ("Provenance selector", true, true, true, true);
		_provView = new ProvenanceViewer(p, s, sys, dataEditFactory);
		add (_provView);
	}
	
	public void close() {
	}
}
