package edu.upenn.cis.orchestra.gui.schemas;

import javax.swing.table.AbstractTableModel;

import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;

public abstract class RelationDataModelAbs extends AbstractTableModel {

	// Relation to show/edit
	final RelationContext _relCtx;
	// Used to communicate with the data layer
	final RelationDataEditorIntf _relDataEdit;
	
	
	/**
	 * Create a new model
	 * @param relDataEdit Used to communicate with the data layer
	 * @param relCtx Relation to edit
	 */
	public RelationDataModelAbs (final RelationDataEditorIntf relDataEdit, 
									final RelationContext relCtx)
	{
		// Copy local attributes
		_relCtx = relCtx;		
		_relDataEdit = relDataEdit;
	}
	
	@Override
	public String getColumnName(int col) {
		RelationField fld = _relCtx.getRelation().getField(col); 
		return fld.getName();
	}
	
	public boolean isPrimaryKey (int col)
	{
		RelationField fld = _relCtx.getRelation().getField(col); 
		return _relCtx.getRelation().getPrimaryKey().getFields().contains(fld);
	}
	
	//TODO: skolems??
	@Override
	public Class<?> getColumnClass(int col) {
		return _relCtx.getRelation().getField(col).getType().getClassObj();
	}

//	@Override
	public int getColumnCount() {
		return _relCtx.getRelation().getFields().size();
	}

	public abstract boolean isSkolem (int row, int col);
	
}
