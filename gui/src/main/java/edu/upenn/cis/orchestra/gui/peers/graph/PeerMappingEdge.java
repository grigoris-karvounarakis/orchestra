package edu.upenn.cis.orchestra.gui.peers.graph;

import java.util.Map;

import javax.swing.UIManager;

import org.jgraph.graph.AttributeMap;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.gui.graphs.GuiDefaultEdge;
import edu.upenn.cis.orchestra.gui.graphs.GuiGraphConstants;
import edu.upenn.cis.orchestra.gui.graphs.GuiParallelEdgeRouter;

public class PeerMappingEdge extends GuiDefaultEdge implements IPeerMapping {
	
	public static final long serialVersionUID = 1L;
	
	private final Mapping _map;
	private final Peer _p;

	public PeerMappingEdge(Peer p, Mapping map) {
		super();
		
		_map = map;
		_p = p;
		
		setNormal(getAttributes());
	}

	public PeerMappingEdge(Peer p, Mapping map, Object arg0, AttributeMap arg1) {
		super(arg0, arg1);
		
		_map = map;
		_p = p;
		
		setNormal(getAttributes());
	}

	public PeerMappingEdge(Peer p, Mapping map, Object arg0) {
		super(arg0);
		
		_map = map;
		_p = p;
		setNormal(getAttributes());
	}

	public void setNormal(Map attr)
	{
		//GuiGraphConstants.setFont(getAttributes(), new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerMapping.normal.font"));//new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerMapping.normal.foreground"));
		GuiGraphConstants.setLayoutEdgeRouting(attr, GuiParallelEdgeRouter.getSharedInstance());
	}
	
	public void setHighlighted(Map attr)
	{
		GuiGraphConstants.setFont(attr, UIManager.getFont("PeerMapping.highlighted.font"));//new Font("SANS SERIF", Font.PLAIN, 12));
		GuiGraphConstants.setForeground(attr, UIManager.getColor("PeerMapping.highlighted.foreground"));
	}
	
	public Mapping getMapping() {
		return _map;
	}

	public Peer getPeer() {
		return _p;
	}
	
	@Override
	public String getTooltipText() {
		return "Mapping " + _map.toString();
	}
	
}
