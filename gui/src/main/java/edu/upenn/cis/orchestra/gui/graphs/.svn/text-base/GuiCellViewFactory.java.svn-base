package edu.upenn.cis.orchestra.gui.graphs;

import java.lang.ref.WeakReference;

import org.jgraph.graph.DefaultCellViewFactory;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.VertexView;

public class GuiCellViewFactory extends DefaultCellViewFactory {

	public static final long serialVersionUID = 1L;
	
	private final WeakReference<BasicGraph> _graph;
	
	public GuiCellViewFactory (BasicGraph graph)
	{
		_graph = new WeakReference<BasicGraph> (graph);
	}

	/*
	 * (non-Javadoc)
	 */
	protected VertexView createVertexView(Object cell) {
		return new GuiVertexView(cell);
	}
	
	@Override
	protected EdgeView createEdgeView(Object edge) {
		return new GuiEdgeView (_graph.get(), edge); 
	}
	
	
}
