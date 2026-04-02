package edu.upenn.cis.orchestra.gui.provenance;

import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.UIManager;

import org.jgraph.graph.DefaultPort;
import org.jgraph.graph.GraphConstants;

import edu.upenn.cis.orchestra.datamodel.Tuple;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultGraphCell;

/**
 * Node representation of a tuple
 * 
 * @author zives
 *
 */
public class TupleVertex extends GuiDefaultGraphCell 
{
	public static final long serialVersionUID = 1L;
	
	private String _label;
	private Tuple _tuple;
	
	public TupleVertex (String label, Tuple t)
	{
		super(label);
		
		_tuple = t;
		_label = label;

		add(new DefaultPort());
		setVisible(getAttributes());
	}
	
	public void setRoot() {
		setRoot(getAttributes());
	}
	
	public static void setRoot(Map attrs) {
		GraphConstants.setBackground(attrs, UIManager.getColor("TupleVertex.root.background"));
		//GraphConstants.setGradientColor(attrs, Color.YELLOW);
		GraphConstants.setForeground(attrs, UIManager.getColor("TupleVertex.root.foreground"));
		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TupleVertex.root.border"), 3));
	}

	public void setEdb() {
		setEdb(getAttributes());
	}
	
	public static void setEdb(Map attrs) {
		GraphConstants.setBackground(attrs, UIManager.getColor("TupleVertex.edb.background"));
		//GraphConstants.setGradientColor(attrs, Color.YELLOW);
		GraphConstants.setForeground(attrs, UIManager.getColor("TupleVertex.edb.foreground"));
		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TupleVertex.edb.border"), 1));
	}
	
	/**
	 * Make the node "dimmed"
	 * 
	 * @param attrs
	 */
	public void setDimmed(Map attrs) {
		GraphConstants.setBackground(attrs, UIManager.getColor("TupleVertex.dimmed.background"));
		GraphConstants.setForeground(attrs, UIManager.getColor("TupleVertex.dimmed.foreground"));
		GraphConstants.setBorderColor(attrs, UIManager.getColor("TupleVertex.dimmed.border"));

		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TupleVertex.dimmed.border"), 1));
			
		GraphConstants.setInset(attrs, 2);
	}
	
	/**
	 * Make the node invisible and borderless
	 * 
	 * @param attrs
	 */
	public static void setInvisible(Map attrs) {
		GraphConstants.setBackground(attrs, UIManager.getColor("TupleVertex.invisible.background"));
		GraphConstants.setForeground(attrs, UIManager.getColor("TupleVertex.invisible.foreground"));
		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TupleVertex.invisible.border"), 1));
	}

	/**
	 * Make the node visible
	 * 
	 * @param attrs
	 */
	public void setVisible(Map attrs) {
		// Set the cell graphic properties
		GraphConstants.setOpaque(attrs, true);
		GraphConstants.setAutoSize(attrs, true);

		GraphConstants.setBackground(attrs, UIManager.getColor("TupleVertex.visible.background"));
		//GraphConstants.setGradientColor(attrs, Color.YELLOW);
		GraphConstants.setForeground(attrs, UIManager.getColor("TupleVertex.visible.foreground"));

		//GraphConstants.setBorder(attrs, BorderFactory.createRaisedBevelBorder());
		GraphConstants.setBorder(attrs, BorderFactory.createLineBorder(UIManager.getColor("TupleVertex.visible.border"), 1));
		GraphConstants.setInset(attrs, 2);
	}
	
	public String toString ()
	{
		return _label;
	}
	
	public Tuple getTuple()
	{
		return _tuple;
	}

	public String getTooltipText() {
		return _tuple.toString();
	}	
}
