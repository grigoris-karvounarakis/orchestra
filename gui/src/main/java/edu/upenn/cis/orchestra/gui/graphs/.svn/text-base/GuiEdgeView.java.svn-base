package edu.upenn.cis.orchestra.gui.graphs;

import java.awt.Shape;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.util.List;
import java.util.Map;

import org.jgraph.JGraph;
import org.jgraph.graph.AttributeMap;
import org.jgraph.graph.CellHandle;
import org.jgraph.graph.CellMapper;
import org.jgraph.graph.CellView;
import org.jgraph.graph.CellViewRenderer;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.GraphCellEditor;
import org.jgraph.graph.GraphContext;
import org.jgraph.graph.GraphModel;

public class GuiEdgeView extends EdgeView {

	public static final long serialVersionUID = 1L;
	
	//Olivier: dirty fix for now: non static since JGraph is buggy with shared renderers
	public final GuiEdgeRenderer _renderer;
	
	public GuiEdgeView (BasicGraph graph)
	{
		this (graph, null);
	}
	
	public GuiEdgeView (BasicGraph graph, Object obj)
	{
		super (obj);
		_renderer = new GuiEdgeRenderer(graph, this); 
	}
	
	@Override
	public CellViewRenderer getRenderer() {
		return _renderer; 
	}
	
	
	protected synchronized Point2D getPointLocation(int index) {
		return super.getPointLocation(index);
	}	


	@Override
	protected synchronized void invalidate() {
		super.invalidate();
	}	
	
	@Override
	public synchronized void addExtraLabel(Point2D arg0, Object arg1) {
		super.addExtraLabel(arg0, arg1);
	}
	
	@Override
	public synchronized void addPoint(int arg0, Point2D arg1) {
		super.addPoint(arg0, arg1);
	}
	
	@Override
	public synchronized Map changeAttributes(Map arg0) {
		return super.changeAttributes(arg0);
	}
	
	@Override
	protected synchronized void checkDefaultLabelPosition() {
		super.checkDefaultLabelPosition();
	}
	
	@Override
	public synchronized void childUpdated() {
		super.childUpdated();
	}
	
	@Override
	protected synchronized AttributeMap createAttributeMap() {
		return super.createAttributeMap();
	}
	
	@Override
	public synchronized AttributeMap getAllAttributes() {
		return super.getAllAttributes();
	}
	
	@Override
	public synchronized AttributeMap getAttributes() {
		return super.getAttributes();
	}	
	
	@Override
	public synchronized Rectangle2D getBounds() {
		return super.getBounds();
	}
	
	@Override
	public synchronized Object getCell() {
		return super.getCell();
	}
	
	@Override
	protected synchronized AttributeMap getCellAttributes(GraphModel arg0) {
		return super.getCellAttributes(arg0);
	}	
	
	@Override
	public synchronized CellView[] getChildViews() {
		return super.getChildViews();
	}
	
	@Override
	public synchronized GraphCellEditor getEditor() {
		return super.getEditor();
	}
	
	
	@Override
	public synchronized Point2D getExtraLabelPosition(int arg0) {
		return super.getExtraLabelPosition(arg0);
	}
	
	@Override
	public synchronized CellHandle getHandle(GraphContext arg0) {
		return super.getHandle(arg0);
	}
	
	@Override
	public synchronized Point2D getLabelPosition() {
		return super.getLabelPosition();
	}
	
	@Override
	public synchronized Point2D getLabelVector() {
		return super.getLabelVector();
	}
	
	@Override
	protected synchronized Point2D getNearestPoint(boolean arg0) {
		return super.getNearestPoint(arg0);
	}
	
	@Override
	public synchronized CellView getParentView() {
		return super.getParentView();
	}
	
	@Override
	public synchronized Point2D getPerimeterPoint(EdgeView arg0, Point2D arg1, Point2D arg2) {
		return super.getPerimeterPoint(arg0, arg1, arg2);
	}
	
	@Override
	public synchronized Point2D getPoint(int arg0) {
		return super.getPoint(arg0);
	}
	
	@Override
	public synchronized int getPointCount() {
		return super.getPointCount();
	}
	
	@Override
	public synchronized List getPoints() {
		return super.getPoints();
	}
	/*
	@Override
	public synchronized Component getRendererComponent(JGraph arg0, boolean arg1, boolean arg2, boolean arg3) {
		return super.getRendererComponent(arg0, arg1, arg2, arg3);
	}
	*/
	@Override
	public Shape getShape() {
		return super.getShape();
	}
	
	@Override
	public synchronized CellView getSource() {
		return super.getSource();
	}
	
	@Override
	public synchronized CellView getSourceParentView() {
		return super.getSourceParentView();
	}
	
	@Override
	public synchronized CellView getTarget() {
		return super.getTarget();
	}
	
	@Override
	public synchronized CellView getTargetParentView() {
		return super.getTargetParentView();
	}
	
	@Override
	protected synchronized CellView getVisibleParent(GraphModel arg0, CellMapper arg1, Object arg2) {
		return super.getVisibleParent(arg0, arg1, arg2);
	}
	
	@Override
	protected synchronized boolean includeInGroupBounds(CellView arg0) {
		return super.includeInGroupBounds(arg0);
	}
	
	@Override
	public synchronized boolean intersects(JGraph arg0, Rectangle2D arg1) {
		return super.intersects(arg0, arg1);
	}
	
	@Override
	public synchronized boolean isLeaf() {
		return super.isLeaf();
	}
	
	@Override
	public synchronized boolean isLoop() {
		return super.isLoop();
	}
	
	@Override
	protected synchronized void mergeAttributes() {
		super.mergeAttributes();
	}
	
	@Override
	public synchronized void refresh(GraphModel arg0, CellMapper arg1, boolean arg2) {
		super.refresh(arg0, arg1, arg2);
	}
	
	@Override
	public synchronized void removeExtraLabel(int arg0) {
		super.removeExtraLabel(arg0);
	}
	
	@Override
	public synchronized void removeFromParent() {
		super.removeFromParent();
	}
	
	@Override
	public synchronized void removePoint(int arg0) {
		super.removePoint(arg0);
	}
	
	@Override
	public synchronized void scale(double arg0, double arg1, Point2D arg2) {
		super.scale(arg0, arg1, arg2);
	}	
	
	@Override
	public synchronized void setAttributes(AttributeMap arg0) {
		super.setAttributes(arg0);
	}
	
	@Override
	public synchronized void setBounds(Rectangle2D arg0) {
		super.setBounds(arg0);
	}
	
	@Override
	public synchronized void setCell(Object arg0) {
		super.setCell(arg0);
	}
	
	@Override
	public synchronized void setExtraLabelPosition(int arg0, Point2D arg1) {
		super.setExtraLabelPosition(arg0, arg1);
	}
	
	@Override
	public synchronized void setLabelPosition(Point2D arg0) {
		super.setLabelPosition(arg0);
	}
	
	@Override
	public synchronized void setPoint(int arg0, Point2D arg1) {
		super.setPoint(arg0, arg1);
	}
	
	@Override
	public synchronized void setSource(CellView arg0) {
		super.setSource(arg0);
	}
	
	@Override
	public synchronized void setTarget(CellView arg0) {
		super.setTarget(arg0);
	}
	
	@Override
	public synchronized void translate(double arg0, double arg1) {
		super.translate(arg0, arg1);
	}
	
	@Override
	public synchronized void update() {
		super.update();
	}
	
	@Override
	protected synchronized void updateGroupBounds() {
		super.updateGroupBounds();
	}
	
	protected void resetLabelPosition ()
	{
		super.checkDefaultLabelPosition();
	}
}



