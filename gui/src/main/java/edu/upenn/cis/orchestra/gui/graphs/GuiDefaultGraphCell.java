package edu.upenn.cis.orchestra.gui.graphs;

import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;


public class GuiDefaultGraphCell 
		extends DefaultGraphCell
		implements GuiDefaultGraphObj
{
	public static final long serialVersionUID = 1L;
	
	public GuiDefaultGraphCell ()
	{
		super ();
		addDefaultPort ();
	}
	
	public GuiDefaultGraphCell(Object usrObject) 
	{
		super (usrObject);
		addDefaultPort ();
	}
	
	public GuiDefaultGraphCell(Object usrObject, AttributeMap attributes)
	{
		super (usrObject, attributes);
		addDefaultPort ();
	}
	
	private void addDefaultPort ()
	{
		DefaultPort port = new DefaultPort ();
		add (port);
		port.setParent(this);		
	}
	
	public DefaultPort getDefaultPort ()
	{
		return (DefaultPort) getChildAt (0);
	}	
	
	public String getTooltipText ()
	{
		return null;
	}
	
	
}
