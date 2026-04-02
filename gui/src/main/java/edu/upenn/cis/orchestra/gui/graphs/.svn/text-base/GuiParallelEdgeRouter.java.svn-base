package edu.upenn.cis.orchestra.gui.graphs;

import java.awt.geom.Point2D;
import java.util.ArrayList;

import org.jgraph.graph.AbstractCellView;
import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.DefaultGraphModel;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.GraphConstants;
import org.jgraph.graph.GraphModel;
import org.jgraph.graph.VertexView;
import org.jgraph.util.Bezier;
import org.jgraph.util.Spline2D;

import java.util.List;

public class GuiParallelEdgeRouter  extends DefaultEdge.LoopRouting
{
	public static final long serialVersionUID = 1L;

	/**
	 * Singleton to reach parallel edge router
	 */
	public static final GuiParallelEdgeRouter sharedInstance = new GuiParallelEdgeRouter();

	/**
	 * Default model
	 */
	private static final GraphModel emptyModel = new DefaultGraphModel();

	/**
	 * Distance between each parallel edge
	 */
	private static double edgeSeparation = 20.;

	/**
	 * Distance between intermediate and source/target points
	 */
	private static double edgeDeparture = 10.;

	/**
	 * Getter for singleton managing parallel edges
	 * 
	 * @return JGraphParallelEdgeRouter Routeur for parallel edges
	 */
	public static GuiParallelEdgeRouter getSharedInstance() {
		return GuiParallelEdgeRouter.sharedInstance;
	}

	/**
	 * Calc of intermediates points
	 * 
	 * @param edge
	 *            Edge for which routing is demanding
	 * @param points
	 *            List of points for this edge
	 */
	@SuppressWarnings("unchecked")
	public List routeEdge(EdgeView edge) {
		List newPoints = new ArrayList();

		// Check presence of source/target nodes
		if ((null == edge.getSource()) || (null == edge.getTarget())
				|| (null == edge.getSource().getParentView())
				|| (null == edge.getTarget().getParentView())) {
			return null;
		}
		newPoints.add(edge.getSource());

		// Check presence of parallel edges
		Object[] edges = getParallelEdges(edge);
		if (edges == null) {
			return null;
		}

		// For one edge, no intermediate point
		if (edges.length >= 2) {

			// Looking for position of edge
			int position = 0;
			for (int i = 0; i < edges.length; i++) {
				Object e = edges[i];
				if (e == edge.getCell()) {
					position = i + 1;
				}
			}

			// Looking for position of source/target nodes (edge=>port=>vertex)
			VertexView nodeFrom = (VertexView) edge.getSource().getParentView();
			VertexView nodeTo = (VertexView) edge.getTarget().getParentView();
			Point2D from = AbstractCellView.getCenterPoint(nodeFrom);
			Point2D to = AbstractCellView.getCenterPoint(nodeTo);

			if (from != null && to != null) {
				double dy = from.getY() - to.getY();
				double dx = from.getX() - to.getX();

				// Calc of radius
				double m = dy / dx;
				double theta = Math.atan(-1 / m);
				double rx = dx / Math.sqrt(dx * dx + dy * dy);
				double ry = dy / Math.sqrt(dx * dx + dy * dy);

				// Memorize size of source/target nodes
				double sizeFrom = Math.max(nodeFrom.getBounds().getWidth(),
						nodeFrom.getBounds().getHeight()) / 2.;
				double sizeTo = Math.max(nodeTo.getBounds().getWidth(), nodeTo
						.getBounds().getHeight()) / 2.;

				// Calc position of central point
				double edgeMiddleDeparture = (Math.sqrt(dx * dx + dy * dy)
						- sizeFrom - sizeTo)
						/ 2 + sizeFrom;

				// Calc position of intermediates points
				double edgeFromDeparture = edgeDeparture + sizeFrom;
				double edgeToDeparture = edgeDeparture + sizeTo;

				// Calc distance between edge and mediane source/target
				double r = edgeSeparation * Math.floor(position / 2);
				if (0 == (position % 2)) {
					r = -r;
				}

				// Convert coordinate
				double ex = r * Math.cos(theta);
				double ey = r * Math.sin(theta);

				// Check if is not better to have only one intermediate point
				if (edgeMiddleDeparture <= edgeFromDeparture) {
					double midX = from.getX() - rx * edgeMiddleDeparture;
					double midY = from.getY() - ry * edgeMiddleDeparture;
					Point2D controlPoint = new Point2D.Double(ex + midX, ey
							+ midY);
					
					// Add intermediate point
					newPoints.add(controlPoint);
					GraphConstants.setLabelPosition(edge.getAllAttributes(), controlPoint);
				} else {
					double midXFrom = from.getX() - rx * edgeFromDeparture;
					double midYFrom = from.getY() - ry * edgeFromDeparture;
					double midXTo = to.getX() + rx * edgeToDeparture;
					double midYTo = to.getY() + ry * edgeToDeparture;

					Point2D controlPointFrom = new Point2D.Double(
							ex + midXFrom, ey + midYFrom);
					Point2D controlPointTo = new Point2D.Double(ex + midXTo, ey
							+ midYTo);

					// Add intermediates points
					newPoints.add(controlPointFrom);
					newPoints.add(controlPointTo);
					
					Point2D[] p = new Point2D[4];
					p[0] = from;
					p[1] = controlPointFrom;
					p[2] = controlPointTo;
					p[3] = to;


					if (GuiGraphConstants.getLineStyle(edge.getAllAttributes())==GuiGraphConstants.STYLE_BEZIER)
					{
						Bezier bz = new Bezier (p);
	
						double x = p[1].getX()*Math.pow(0.5,3)
									+ 3*bz.getPoint(1).getX()*Math.pow(0.5,3)
									+ 3*bz.getPoint(2).getX()*Math.pow(0.5,3)
									+ p[2].getX()*Math.pow(0.5,3);
						double y = p[1].getY()*Math.pow(0.5,3)
									+ 3*bz.getPoint(1).getY()*Math.pow(0.5,3)
									+ 3*bz.getPoint(2).getY()*Math.pow(0.5,3)
									+ p[2].getY()*Math.pow(0.5,3);
						GraphConstants.setLabelPosition(edge.getAttributes(), new Point2D.Double(x,y));
					}
					else
					{
						Spline2D spline = new Spline2D(p);					
						double[] pt = spline.getPoint(0.5);
						GraphConstants.setLabelPosition(edge.getAttributes(), new Point2D.Double(pt[0],pt[1]));
					}
				}
			}
		}
		newPoints.add(edge.getTarget());
		return newPoints;
	}

	/**
	 * Getter to obtain the distance between each parallel edge
	 * 
	 * @return Distance
	 */
	public static double getEdgeSeparation() {
		return GuiParallelEdgeRouter.edgeSeparation;
	}

	/**
	 * Setter to define distance between each parallel edge
	 * 
	 * @param edgeSeparation
	 *            New distance
	 */
	public static void setEdgeSeparation(double edgeSeparation) {
		GuiParallelEdgeRouter.edgeSeparation = edgeSeparation;
	}

	/**
	 * Getter to obtain the distance between intermediate and source/target
	 * points
	 * 
	 * @return Distance
	 */
	public static double getEdgeDeparture() {
		return GuiParallelEdgeRouter.edgeDeparture;
	}

	/**
	 * Setter to define distance between intermediate and source/target points
	 * 
	 * @param edgeDeparture
	 *            New distance
	 */
	public static void setEdgeDeparture(double edgeDeparture) {
		GuiParallelEdgeRouter.edgeDeparture = edgeDeparture;
	}

	/**
	 * Getter to obtain the list of parallel edges
	 * 
	 * @param edge
	 *            Edge on which one wants to know parallel edges
	 * @return Object[] Array of parallel edges (include edge passed on
	 *         argument)
	 */
	private Object[] getParallelEdges(EdgeView edge) {
		return DefaultGraphModel.getEdgesBetween(emptyModel, edge.getSource()
				.getParentView().getCell(), edge.getTarget().getParentView()
				.getCell(), false);
	}

}
