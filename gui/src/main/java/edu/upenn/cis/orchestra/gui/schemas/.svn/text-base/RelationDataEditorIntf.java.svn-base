package edu.upenn.cis.orchestra.gui.schemas;

import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.datamodel.TxnPeerID;
import edu.upenn.cis.orchestra.datamodel.Update;
import edu.upenn.cis.orchestra.datamodel.iterators.ResultIterator;

public interface RelationDataEditorIntf 
{
	public ResultIterator<Tuple> getData (RelationContext relationCtx)
					throws RelationDataEditorException; 
	
	// Single update transaction 
	public void addUpdate (RelationContext relationCtx, Update update)
					throws RelationDataEditorException;
	
	public TxnPeerID addTransaction (RelationContext relationCtx, List<Update> transaction)
					throws RelationDataEditorException;

	public TxnPeerID addTransaction (Map<RelationContext,List<Update>> transaction)
					throws RelationDataEditorException;
	
	public Relation getRelationSchema (RelationContext relationCtx)
					throws RelationDataEditorException;
	
	public boolean isKeyUsed(RelationContext relCtx, Tuple row)
					throws RelationDataEditorException;

}
