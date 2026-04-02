package edu.upenn.cis.orchestra.gui.graphs;

import java.awt.Dimension;
import java.awt.Point;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.awt.geom.Point2D;
import java.awt.geom.Rectangle2D;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.swing.JViewport;
import javax.swing.SwingWorker;
import javax.swing.ToolTipManager;

import org.jgraph.JGraph;
import org.jgraph.graph.CellView;
import org.jgraph.graph.DefaultEdge;
import org.jgraph.graph.DefaultGraphModel;
import org.jgraph.graph.EdgeView;
import org.jgraph.graph.GraphLayoutCache;
import org.jgraph.plaf.basic.BasicGraphUI;

import com.jgraph.layout.JGraphFacade;
import com.jgraph.layout.JGraphLayout;

public abstract class BasicGraph extends JGraph {

	
	public static final int DEFAULT_WIDTH = 800;
	public static final int DEFAULT_HEIGHT = 600;
	
	private boolean _enableRoutingWhenNoLayout = true;
	
	protected class BooleanWrap
	{
		private boolean _val = false;
		

		public synchronized boolean setVal (boolean val)
		{
			if (val = _val)
				return false;
			_val = val;
			return true;
				
		}
		
		public synchronized boolean getVal ()
		{
			return _val;
		}
		

	}
	
	
	private BooleanWrap _currlayout = new BooleanWrap (); 
	
	public BasicGraph ()
	{
		this (false);
		
	}
	
	public BasicGraph (boolean useTooltips)
	{
		super (new DefaultGraphModel ());
		
		
		getGraphLayoutCache().setSelectsAllInsertedCells (false);
		getGraphLayoutCache().setSelectsLocalInsertedCells (false);
		getGraphLayoutCache().setAutoSizeOnValueChange(true);
		
		setUI(new SpecGraphUI());
		
		setAntiAliased(true);

		
		getGraphLayoutCache().setFactory(new GuiCellViewFactory (this));
		
		// Set graph properties for user interaction
		setEditable(false);
		setDisconnectable(false);
		setAntiAliased(true);
		setDropEnabled(false);
		setEdgeLabelsMovable(false);
		
		if (useTooltips)
			ToolTipManager.sharedInstance().registerComponent(this);
	}
	

	@Override
	public String getToolTipText(MouseEvent event) 
	{
		Object cell = getFirstCellForLocation(event.getX(), event.getY());
		  if (cell instanceof GuiDefaultGraphObj) {
		    return ((GuiDefaultGraphObj) cell).getTooltipText();
		  }
		  return null;
	}
	
	public SpecGraphUI getSpecializedGraphUI ()
	{
		return (SpecGraphUI) getUI();
	}
	
	public void setUI(SpecGraphUI arg0) {
		super.setUI(arg0);
	}
	
	public boolean isCurrLayout ()
	{
		return _currlayout.getVal();
	}
	
	public boolean setCurrLayout (boolean val)
	{
		return _currlayout.setVal(val);
	}
	
	public abstract void applyLayout ();
	

	@SuppressWarnings("unchecked")
	public void applyLayout(final JGraphFacade facade,
									final JGraphLayout layout, 
									final boolean tiltFirst, 
									final Point originFlush) 
	{
		applyLayout(facade, layout, tiltFirst, 1, originFlush);
	}

	@SuppressWarnings("unchecked")
	public void applyLayout(final JGraphFacade facade,
									final JGraphLayout layout, 
									final boolean tiltFirst, 
									final int nbRuns,
									final Point originFlush) 
	{
		applyLayout(facade, layout, tiltFirst, nbRuns, originFlush, false);
	}

	
	@SuppressWarnings("unchecked")
	public void applyLayout(final JGraphFacade facade,
									final JGraphLayout layout, 
									final boolean tiltFirst,
									final Point originFlush,
									final boolean recenter) 
	{
		applyLayout(facade, layout, tiltFirst, 1, originFlush, recenter);		
	}
	
	
	@SuppressWarnings("unchecked")
	public void applyLayout(final JGraphFacade facade,
									final JGraphLayout layout, 
									final boolean tiltFirst,
									final int nbRuns,
									final Point originFlush,
									final boolean recenter) 
	{


		final JGraph graph = this;  
		
		SwingWorker<Void, Void> worker = new SwingWorker<Void, Void> ()
		{
			@Override
			protected Void doInBackground() throws Exception {
				try
				{
					while (!_currlayout.setVal(true))
						Thread.sleep(10);
					synchronized (getTreeLock()) { synchronized (this) {
						enableRouting(facade);
					
						if (tiltFirst)
						{
							List cells = new ArrayList (facade.getVertices().size());
							cells.addAll(facade.getVertices());
							facade.tilt(cells, DEFAULT_WIDTH, DEFAULT_HEIGHT);
							Map nested = facade.createNestedMap(true, true); 
							getGraphLayoutCache().edit(nested);
						}
		
						for (int i = 0 ; i < nbRuns ; i++)
						{
							layout.run(facade);
			
							Map map = facade.createNestedMap(true, originFlush);
							getGraphLayoutCache().edit(map);
						}
						
						if (recenter)
							recenter (facade);
		
						if (!_enableRoutingWhenNoLayout)
							disableRouting(facade);
						
						getGraphLayoutCache().reload();
					}}
				} catch (Exception e) {
					e.printStackTrace();
				} 
				return null;
	
			}
			
			@Override
			protected void done() {

				_currlayout.setVal(false); 					
				graph.repaint ();
			}
		};
		//Tmp, still need to fix renderer
		worker.execute();
		
		
		

	}
	
	protected void disableRouting ()
	{
		JGraphFacade facade = new JGraphFacade (this);
		disableRouting(facade);
	}
	
	protected void disableRouting (JGraphFacade facade)
	{
		for (Object edgeo : facade.getEdges())
		{
			DefaultEdge edge = (DefaultEdge) edgeo;
			if (GuiGraphConstants.getLayoutEdgeRouting(edge.getAttributes())!=null)
			{
				GuiGraphConstants.setPoints(edge.getAttributes(), ((EdgeView) facade.getCellView(edge)).getPoints());
				edge.getAttributes().remove(GuiGraphConstants.ROUTING);			
			}
			getGraphLayoutCache().editCell (edge, edge.getAttributes());
		}		
	}
	
	protected void enableRouting ()
	{
		JGraphFacade facade = new JGraphFacade (this);
		enableRouting(facade);
		
	}
	
	@SuppressWarnings("unchecked")
	protected void enableRouting (JGraphFacade facade)
	{
		List edgesO = new ArrayList (facade.getEdges());
		for (Object edgeo : edgesO)
		{
			DefaultEdge edge = (DefaultEdge) edgeo;
			if (GuiGraphConstants.getLayoutEdgeRouting(edge.getAttributes())!=null)
				GuiGraphConstants.setRouting(edge.getAttributes(), GuiGraphConstants.getLayoutEdgeRouting(edge.getAttributes()));
			getGraphLayoutCache().editCell(edge, edge.getAttributes());
		}
		
	}
	
	public void setEnableRoutingWhenNotLayout (boolean val)
	{
		if (_enableRoutingWhenNoLayout != val)
		{
			if (val)
				enableRouting();
			else
				disableRouting();
		}
		_enableRoutingWhenNoLayout = val;		
	}
	
	public boolean isEnableRoutingWhenNotLayout ()
	{
		return _enableRoutingWhenNoLayout; 
	}
	
	@SuppressWarnings("unchecked")
	public void center ()
	{
		if (getParent()!=null)
		{
			Dimension viewBounds;
			if (getParent() instanceof JViewport)
				viewBounds = ((JViewport) getParent()).getExtentSize();
			else
				viewBounds = getParent().getSize();		
			Rectangle2D graphBounds = GraphLayoutCache.getBounds(getGraphLayoutCache().getCellViews());
		
			double dx = -graphBounds.getX(); 
			dx += viewBounds.getWidth()>graphBounds.getWidth()?(((viewBounds.getWidth() - graphBounds.getWidth()) / 2)):0;
			double dy = -graphBounds.getY();
			dy += viewBounds.getHeight()>graphBounds.getHeight()?(((viewBounds.getHeight() - graphBounds.getHeight()) / 2)):0;
			
			Map nestedChanges = new HashMap ();
			for (CellView view : getGraphLayoutCache().getCellViews())
			{
				Rectangle2D cellBounds = view.getBounds();
				Map atts = new HashMap (2);
				GuiGraphConstants.setBounds(atts, new Rectangle2D.Double(cellBounds.getX()+dx,
																cellBounds.getY()+dy,
																cellBounds.getWidth(),
																cellBounds.getHeight()));

				nestedChanges.put(view.getCell(), atts);
				
				if (getModel().isEdge(view.getCell()))
				{
					for (Object obj : GuiGraphConstants.getPoints(view.getAllAttributes()))
					{
						if (obj instanceof Point2D)
						{
							Point2D pt = (Point2D) obj;
							pt.setLocation(pt.getX()+dx, pt.getY()+dy);
						}
					}
					/*
					 * 
					 Point2D labPos = GuiGraphConstants.getLabelPosition(view.getAllAttributes());
					if (labPos!=null)
						GuiGraphConstants.setLabelPosition(atts, new Point2D.Double(labPos.getX()+dx,labPos.getY()+getY()));
					*/
				}
			}
			getGraphLayoutCache().edit(nestedChanges);
			getGraphLayoutCache().reload();
			revalidate();
			repaint();
			
		}		
		
		
	}
	
	public void recenter(JGraphFacade facade) {
		

		if (getParent()!=null)
		{
				Dimension bounds;
				if (getParent() instanceof JViewport)
					bounds = ((JViewport) getParent()).getExtentSize();
				else
					bounds = getParent().getSize();
		
				Rectangle2D rect = facade.getCellBounds();

				if (rect != null)
				{
		
					int offsetX = bounds.getWidth()>rect.getWidth()?((int)((bounds.getWidth() - rect.getWidth()) / 2)):0;
					int offsetY = bounds.getHeight()>rect.getHeight()?((int)((bounds.getHeight() - rect.getHeight()) / 2)):0;
					
					if (offsetX < 10)
						offsetX = 10;
					
					if (offsetY < 10)
						offsetY = 10;
					
					Map nested = facade.createNestedMap(true, new Point(offsetX,offsetY)); 
					getGraphLayoutCache().edit(nested);
				}
		}
	}
	
	
	/**
	 * Extend the default graph UI to add mouse listeners and not to have to 
	 * extend graphUI at each time!
	 * 
	 * @author Olivier Biton
	 *
	 */
	public class SpecGraphUI extends BasicGraphUI
	{
		public static final long serialVersionUID = 1L;
		
		private List<MouseListener> _listeners = new ArrayList<MouseListener> (2);
		
		/**
		 * Override the default listener to notify our own list of listeners 
		 * @return Mouse listener New mouse listener
		 */
		@Override
		protected MouseListener createMouseListener() {
			return new SkeletonUIMouseHandler();
		}
		
		public void addMouseListener (MouseListener mList)
		{
			_listeners.add (mList);
		}
		
		public void removeMouseListener (MouseListener mList)
		{
			_listeners.remove (new WeakReference<MouseListener> (mList));
		}
		
		/**
		 * This class is used to catch the double click on bio step classes
		 * @author Olivier Biton
		 *
		 */
	    private class SkeletonUIMouseHandler
	        extends BasicGraphUI.MouseHandler 
	    {
	    	public static final long serialVersionUID = 1L;
	    
	        /**
	         * Method called on mousePressed event.
	         * @param e Mouse pressed event...
	         */
	        @Override
			public void mousePressed(MouseEvent e) 
	        {
	            super.mousePressed (e);
	        	for (MouseListener list : _listeners)
        			list.mousePressed(e);
	        }

	        @Override
	        public void mouseClicked(MouseEvent e) {
	        	super.mouseClicked(e);
	        	for (MouseListener list : _listeners)
        			list.mouseClicked(e);
	        }
	        
	        @Override
	        public void mouseEntered(MouseEvent e) {
	        	super.mouseEntered(e);
	        	for (MouseListener list : _listeners)
        			list.mouseEntered(e);
	        }
	        
	        @Override
	        public void mouseExited(MouseEvent e) {
	        	super.mouseExited(e);
	        	for (MouseListener list : _listeners)
        			list.mouseExited(e);
	        }
	        
	        @Override
	        public void mouseReleased(MouseEvent e) {
	        	super.mouseReleased(e);
	        	for (MouseListener list : _listeners)
        			list.mouseReleased(e);
	        }
	        
	        
	        
	       
	        
	    }
	}		
	
	
}
