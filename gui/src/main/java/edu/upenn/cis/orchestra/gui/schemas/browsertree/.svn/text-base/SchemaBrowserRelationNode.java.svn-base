package edu.upenn.cis.orchestra.gui.schemas.browsertree;

import javax.swing.tree.DefaultMutableTreeNode;

import edu.upenn.cis.orchestra.datamodel.Relation;

public class SchemaBrowserRelationNode extends DefaultMutableTreeNode 
{
	public static final long serialVersionUID = 1L;
	
	private final Relation _rel;
	
	public SchemaBrowserRelationNode (final Relation rel)
	{
		super (rel.getName());
		_rel = rel;		
	}
	
	public Relation getRelation ()
	{
		return _rel;
	}

}
