/**
 * Exceptions related to the Orchestra Repository Model API
 */
package edu.upenn.cis.orchestra.datamodel.exceptions;


/**
 * Exception generated when trying to add a mapping to a peer while 
 * there is already a mapping with the same id
 * @author Olivier Biton
 *
 */
public class DuplicateMappingIdException extends ModelException {

	private static final long serialVersionUID = 1L;
	
	private String _peerId;
	private String _mappingId;
	
	public DuplicateMappingIdException (String peerId, String mappingId)
	{
		super ("Mapping id " + mappingId + " is already used in peer " + peerId);
	}

	public String getMappingId() {
		return _mappingId;
	}

	public String getPeerId() {
		return _peerId;
	}
}
