package edu.upenn.cis.orchestra.gui.graphs;

import java.util.Map;

import javax.swing.SwingConstants;

import com.jgraph.layout.JGraphFacade;
import com.jgraph.layout.tree.JGraphTreeLayout;

/**
 * The visualization of transactions, dependencies, and conflicts
 * 
 * @author zives
 *
 */
public class LegendGraph extends BasicGraph {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	Object[] _roots;
	
	public LegendGraph() {
		super();
		setEditable(false);
		setSelectionEnabled(false);
	}
	
	public void setRoots(Object[] r) {
		_roots = r;
	}
	
	public Object[] getRoots() {
		return _roots;
	}
	
	@SuppressWarnings("unchecked")
	public void applyLayout ()
	{
		JGraphFacade facade = new JGraphFacade (this, getRoots());
		//facade.tilt(c, width / 2 + 1, height / 2 + 1);
		
		JGraphTreeLayout hLayout = new JGraphTreeLayout();
		hLayout.setOrientation(SwingConstants.WEST);
		hLayout.setLevelDistance(120);//width / 12);
		hLayout.run(facade);
		Map nested = facade.createNestedMap(true, true); 
		getGraphLayoutCache().edit(nested);
	}
}
