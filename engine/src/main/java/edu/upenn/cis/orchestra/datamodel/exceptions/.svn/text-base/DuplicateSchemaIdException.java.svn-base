package edu.upenn.cis.orchestra.datamodel.exceptions;


/**
 * Exception generated when trying to add a schema to a peer while 
 * there is already a schema with the same id
 * @author Olivier Biton
 *
 */
public class DuplicateSchemaIdException extends ModelException {
	public static final long serialVersionUID = 1L;
	
	private String _peerId;
	private String _schemaId;
	
	public DuplicateSchemaIdException (String peerId, String schemaId)
	{
		super ("Schema id " + schemaId + " is already used in peer " + peerId);
		_peerId = peerId;
		_schemaId = schemaId;
	}

	public String getPeerId() {
		return _peerId;
	}

	public String getSchemaId() {
		return _schemaId;
	}
}
