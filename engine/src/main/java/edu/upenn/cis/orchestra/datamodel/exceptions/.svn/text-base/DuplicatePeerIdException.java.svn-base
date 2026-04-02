package edu.upenn.cis.orchestra.datamodel.exceptions;

/**
 * Exception generated when trying to add a peer while 
 * there is already a peer with the same id
 * @author Olivier Biton
 *
 */
public class DuplicatePeerIdException extends ModelException 
{
	public static final long serialVersionUID = 1L; 

	private String _peerId;
	
	public DuplicatePeerIdException (String peerId)
	{
		super ("Peer id <" + peerId + "> already exists");
		_peerId=peerId;
	}

	public String getPeerId() {
		return _peerId;
	}
	
}
