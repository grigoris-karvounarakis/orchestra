package edu.upenn.cis.orchestra.gui.schemas;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;

public class RelationDataEditorFactory {

	private final RelationDataEditorIntf _dataEdit; 
	
	public RelationDataEditorFactory (OrchestraSystem sys)
	{
		if (sys.getRecMode())
			_dataEdit = getReconInstance(sys);
		else
			_dataEdit = getUpdTransInstance(sys);
	}

	public RelationDataEditorIntf getInstance (OrchestraSystem sys)
	{
		return _dataEdit;
	}

	private static RelationDataEditorIntf getReconInstance (OrchestraSystem sys)
	{
		return new RelationDataEditor (sys);
	}

	private static RelationDataEditorIntf getUpdTransInstance (OrchestraSystem sys)
	{
		return new RelationDataEditorUpdTrans (sys);
	}

}
