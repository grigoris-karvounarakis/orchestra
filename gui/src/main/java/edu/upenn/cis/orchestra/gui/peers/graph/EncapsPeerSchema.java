package edu.upenn.cis.orchestra.gui.peers.graph;

import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Schema;

public class EncapsPeerSchema 
{
	private Peer _p;
	private Schema _s;
	
	public EncapsPeerSchema (Peer p, Schema s)
	{
		_p = p;
		_s = s;
	}
	
	public Peer getPeer ()
	{
		return _p;			
	}
	
	public Schema getSchema ()
	{
		return _s;
	}
	
}