package edu.upenn.cis.orchestra.datamodel.exceptions;


/**
 * Raised if an object (such as a constraint, a foreign key) references
 * a field unknown in the system
 * @author Olivier Biton
 *
 */
public class UnknownRefFieldException extends ModelException {
	private static final long serialVersionUID = 1L;
	
	public UnknownRefFieldException (String msg)
	{
		super (msg);
	}
}
