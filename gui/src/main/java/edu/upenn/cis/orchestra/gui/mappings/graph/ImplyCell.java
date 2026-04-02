package edu.upenn.cis.orchestra.gui.mappings.graph;

import java.awt.Color;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultGraphCell;
import org.jgraph.graph.DefaultPort;

import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexRenderer;

public class ImplyCell extends DefaultGraphCell {

	public static final long serialVersionUID = 1L;
	public static final int IMPLY_CELL_SHAPE = GuiVertexRenderer.SHAPE_CIRCLE;
	
	public ImplyCell ()
	{
		super ();
		
		// Set the cell graphic properties
		GuiGraphConstants.setOpaque(getAttributes(), true);
		GuiGraphConstants.setBackground(getAttributes(), UIManager.getColor("ImplyCell.background"));
		GuiGraphConstants.setForeground(getAttributes(), UIManager.getColor("ImplyCell.foreground"));
		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(UIManager.getColor("ImplyCell.border"), 1));
		GuiGraphConstants.setAutoSize(getAttributes(), true);
		GuiGraphConstants.setInset(getAttributes(), 2);
		GuiGraphConstants.setVertexShape(getAttributes(), ImplyCell.IMPLY_CELL_SHAPE);
		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(Color.BLACK, 6));
		
		// Create a default port
		DefaultPort port = new DefaultPort ();
		add (port);
		port.setParent(this);		
	}
	
}
