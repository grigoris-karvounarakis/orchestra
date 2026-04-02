package edu.upenn.cis.orchestra.gui.schemas.graph;

import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.ForeignKey;

public class ForeignKeyEdge extends DefaultEdge 
{
	public static final long serialVersionUID = 1L;
	
	public ForeignKeyEdge (ForeignKey fk)
	{
		super (fk);
		
		GraphConstants.setLineEnd(getAttributes(), GraphConstants.ARROW_CLASSIC);
		GraphConstants.setLabelAlongEdge(getAttributes(), true);
	}
	
	public ForeignKey getForeignKey ()
	{
		return (ForeignKey) getUserObject();
	}
	
	public String toString ()
	{
		ForeignKey fk = getForeignKey();
		return fk.getName();
	}
	

}
