package edu.upenn.cis.orchestra.repository.dao.exceptions;

public class UnknownPeerException extends DAOException 
{
	public static final long serialVersionUID=1L;

	private String _peerId;
	
	public UnknownPeerException (String peerId)
	{
		super ("Peer " + peerId + " is unknown");
	}
	
	public String getPeerId ()
	{
		return _peerId;
	}
}
