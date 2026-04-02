package edu.upenn.cis.orchestra.gui.graphs;

import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.DefaultEdge;

public class GuiDefaultEdge extends DefaultEdge 
							implements GuiDefaultGraphObj {

	private static final long serialVersionUID = 1L;

	public GuiDefaultEdge ()
	{
		super ();
	}
	
	public GuiDefaultEdge (Object usrObject)
	{
		super (usrObject);
	}
	
	public GuiDefaultEdge (Object usrObject, AttributeMap attributes)
	{
		super (usrObject, attributes);
	}
	
	public String getTooltipText() 
	{
		return null;
	}
}

