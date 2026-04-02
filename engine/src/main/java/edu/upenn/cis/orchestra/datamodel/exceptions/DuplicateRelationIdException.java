package edu.upenn.cis.orchestra.datamodel.exceptions;


//TODO: add peer id !
/**
 * Exception generated when trying to add a relation to a schema while 
 * there is already a relation with the same id
 * @author Olivier Biton
 *
 */
public class DuplicateRelationIdException extends ModelException {
	public static final long serialVersionUID = 1L; 
	
	String _relId;
	String _schemaId;
	
	public DuplicateRelationIdException (String relId, String schemaId)
	{
		super ("Relation id " + relId + " is already used in schema " + schemaId);
		_schemaId = schemaId;
		_relId = relId;
	}

	public String getRelId() {
		return _relId;
	}

	public String getSchemaId() {
		return _schemaId;
	}
	
}
