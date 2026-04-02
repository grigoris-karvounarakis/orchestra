package edu.upenn.cis.orchestra.gui.graphs;

import java.util.Map;

import org.jgraph.graph.GraphConstants;
import org.jgraph.graph.Edge.Routing;

public class GuiGraphConstants extends GraphConstants {

	/**
	 * Key for the <code>vertexShape</code> attribute. This special attribute
	 * contains an Integer instance indicating which shape should be drawn by
	 * the renderer.
	 */
	public final static String VERTEXSHAPE = "vertexShape";

	/**
	 * Key for the <code>stretchImage</code> attribute. This special attribute
	 * contains a Boolean instance indicating whether the background image
	 * should be stretched.
	 */
	public final static String STRETCHIMAGE = "stretchImage";
	
	/**
	 * Key for an edge router to use only while applying the layout, will 
	 * work only if using the applyLayout methods from BasicGraph
	 */
	public final static String LAYOUTEDGEROUTING = "layoutEdgeRouting";
	
	
	/**
	 * Returns true if stretchImage in this map is true. Default is false.
	 */
	public static final boolean isStretchImage(Map map) {
		Boolean boolObj = (Boolean) map.get(STRETCHIMAGE);
		if (boolObj != null)
			return boolObj.booleanValue();
		return false;
	}

	/**
	 * Sets stretchImage in the specified map to the specified value.
	 */
	@SuppressWarnings("unchecked")
	public static final void setStretchImage(Map map, boolean stretchImage) {
		map.put(STRETCHIMAGE, new Boolean(stretchImage));
	}
	
	
	/**
	 * Sets vertexShape in the specified map to the specified value.
	 */
	@SuppressWarnings("unchecked")
	public static final void setVertexShape(Map map, int shape) {
		map.put(VERTEXSHAPE, new Integer(shape));
	}

	/**
	 * Returns vertexShape from the specified map.
	 */
	public static final int getVertexShape(Map map) {
		Integer intObj = (Integer) map.get(VERTEXSHAPE);
		if (intObj != null)
			return intObj.intValue();
		return 0;
	}
	
	/**
	 * 
	 */
	@SuppressWarnings("unchecked")
	public static void setLayoutEdgeRouting (Map map, Routing routing)
	{
		map.put(LAYOUTEDGEROUTING, routing);
	}
	
	public static Routing getLayoutEdgeRouting (Map map)
	{
		return (Routing) map.get(LAYOUTEDGEROUTING);
	}


}
