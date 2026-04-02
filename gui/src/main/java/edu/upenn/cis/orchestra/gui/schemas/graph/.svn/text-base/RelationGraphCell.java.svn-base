package edu.upenn.cis.orchestra.gui.schemas.graph;

import javax.swing.BorderFactory;
import javax.swing.UIDefaults;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.Relation;

public class RelationGraphCell extends DefaultGraphCell 
{
	public static final long serialVersionUID = 1L;
	
	public RelationGraphCell (Relation rel)
	{
		super (rel);
		UIDefaults def = UIManager.getDefaults();
		
		// Set the cell graphic properties
		GraphConstants.setOpaque(getAttributes(), true);
		GraphConstants.setBackground(getAttributes(), def.getColor("Relation.background"));
		GraphConstants.setForeground(getAttributes(), def.getColor("Relation.foreground"));
		GraphConstants.setFont(getAttributes(), def.getFont("Relation.font"));
		GraphConstants.setAutoSize(getAttributes(), true);
//		GraphConstants.setBorder(getAttributes(), BorderFactory.createRaisedBevelBorder());
		GraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(UIManager.getColor("Relation.border"), 1));
		GraphConstants.setInset(getAttributes(), 2);
	}
	
	public Relation getRelation ()
	{
		return (Relation) super.getUserObject();
	}
	
	public String toString ()
	{
		Relation rel = getRelation();
		return rel.getName();
	}

}
