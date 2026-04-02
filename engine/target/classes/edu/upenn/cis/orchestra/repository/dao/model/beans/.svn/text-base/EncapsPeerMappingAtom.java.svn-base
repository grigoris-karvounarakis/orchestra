package edu.upenn.cis.orchestra.repository.dao.model.beans;

import edu.upenn.cis.orchestra.repository.model.beans.PeerBean;
import edu.upenn.cis.orchestra.repository.model.beans.ScMappingBean;

public class EncapsPeerMappingAtom extends EncapsPeerMapping {
	private ScMappingAtomTextBean _atom;
	private boolean _isHead;
	
	public EncapsPeerMappingAtom ()
	{
		super ();
	}
	
	public EncapsPeerMappingAtom (PeerBean peer, ScMappingBean mapping, ScMappingAtomTextBean atom, boolean isHead)
	{
		super (peer, mapping);
		this._atom = atom;
		this._isHead = isHead;
	}

	public ScMappingAtomTextBean getAtom() {
		return _atom;
	}

	public void setAtom(ScMappingAtomTextBean atom) {
		this._atom = atom;
	}

	public boolean isHead() {
		return _isHead;
	}

	public void setHead(boolean isHead) {
		this._isHead = isHead;
	}



	
	
	
	
	
}
