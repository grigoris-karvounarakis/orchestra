package edu.upenn.cis.orchestra.gui.mappings.graph;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultPort;

import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiVertexRenderer;

public class TableCell extends GuiDefaultGraphCell {

	public static final long serialVersionUID = 1L;

	private RelationContext _relCtx;
	private String _parameters;

	// Constants for relation cells in the graph
	public static final int TABLE_CELL_SHAPE = GuiVertexRenderer.SHAPE_RECTANGLE;
	
	public TableCell(Atom atom, boolean isBody)
	{
		super (atom.getRelationContext().getRelation().getName());
		
		_relCtx = atom.getRelationContext();
		
		StringBuffer params = new StringBuffer ();
		for (AtomArgument val : atom.getValues())
			params.append((params.length()==0?"":",") + val.toString());
		_parameters = params.toString();
		
		// Set the cell graphic properties
		String background = isBody ? "TableCell.body.background" : "TableCell.head.background";
		String foreground = isBody ? "TableCell.body.foreground" : "TableCell.head.foreground";
		String border = isBody ? "TableCell.body.border" : "TableCell.head.border";

		GuiGraphConstants.setBackground(getAttributes(), UIManager.getColor(background));
		GuiGraphConstants.setForeground(getAttributes(), UIManager.getColor(foreground));
		GuiGraphConstants.setBorder(getAttributes(), BorderFactory.createLineBorder(UIManager.getColor(border), 1));

		GuiGraphConstants.setOpaque(getAttributes(), true);
		GuiGraphConstants.setAutoSize(getAttributes(), true);
		GuiGraphConstants.setInset(getAttributes(), 2);
		GuiGraphConstants.setVertexShape(getAttributes(), TableCell.TABLE_CELL_SHAPE);
		
		// Create a default port
		DefaultPort port = new DefaultPort ();
		add (port);
		port.setParent(this);
	}

	@Override
	public String getTooltipText() {
		return _relCtx.toString() + "(" + _parameters + ")";
	}

}
