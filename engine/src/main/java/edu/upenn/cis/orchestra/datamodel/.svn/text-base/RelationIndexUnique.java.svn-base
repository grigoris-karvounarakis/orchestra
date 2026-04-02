package edu.upenn.cis.orchestra.datamodel;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;

/****************************************************************
 * Unique index definition
 * @author Olivier Biton
 *****************************************************************
 */
public class RelationIndexUnique extends RelationIndex {
	private static final long serialVersionUID = 1L;

	/**
	 * Create a new unique index
	 * @param name Constraint name
	 * @param rel Relation containing the unique index
	 * @param fields Fields concerned by the UNIQUE constraint
	 * @throws UnknownRefFieldException If a unique index field is unknown
	 */
	public RelationIndexUnique(String name, Relation rel, List<String> fields)
			throws UnknownRefFieldException
   {
		super (name, rel, fields);
   }	

	/**
	 * Create a new unique index
	 * @param name Constraint name
	 * @param rel Relation containing the unique index
	 * @param fields Fields concerned by the UNIQUE constraint
     * @param nbUniqueVals Statistics: number of unique values for this index
	 * @throws UnknownRefFieldException If a unique index field is unknown
	 */
	public RelationIndexUnique(String name, Relation rel, String[] fields, int nbUniqueVals)
			throws UnknownRefFieldException
   {
		super (name, rel, fields);
   }		
	
	/**
	 * Create a new unique index
	 * @param name Constraint name
	 * @param rel Relation containing the unique index
	 * @param fields Fields concerned by the UNIQUE constraint
	 * @throws UnknownRefFieldException If a unique index field is unknown
	 */
	public RelationIndexUnique(String name, Relation rel, String[] fields)
			throws UnknownRefFieldException
   {
		super (name, rel, fields);
   }	

   /**
    * New unique index, from bean.
    * @param constBean Bean describing the constraint
    * @param rel Relation to which the constraint is related
    * @throws InvalidBeanException If a field does not exist
    */
   public RelationIndexUnique (ScConstraintBean cstBean, AbstractRelation rel)
   				throws InvalidBeanException
   {
	   super (cstBean, rel);
   }	
	   	
   /**
    * Returns a description of the unique index, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Unique index description
    */       	
	public String toString ()
    {
	   String description = "UNIQUE INDEX " + super.toString();
	   return description;
    }
	
	/**
	 * Sets the bean properties, used to communicate with the DAO layer.
	 * @param bean the bean for which to set properties
	 * @see IntegrityConstraint#toBean()
	 */
	protected void setBeanProperties (ScConstraintBean bean)
	{
		super.setBeanProperties(bean);
		bean.setType("U");
	} 	

}
