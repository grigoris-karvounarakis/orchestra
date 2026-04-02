package edu.upenn.cis.orchestra.datamodel;


import java.io.Serializable;
import java.util.List;

import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;


/**
 * This class has been introduced for specific index information such as index statistics.
 * Even though an index when it's non unique is not a constraint it's easier to have all the indexes
 * inheriting from constraint since the attributes are the same 
 * @author Olivier Biton
 *
 */
public abstract class RelationIndex extends IntegrityConstraint implements Serializable {
	public static final long serialVersionUID = 1L;
	
   /**
    * New index, from bean.
    * @param constBean Bean describing the constraint
    * @param rel Relation to which the constraint is related
    * @throws InvalidBeanException If a field does not exist
    */
	public RelationIndex(ScConstraintBean constBean, AbstractRelation rel) throws InvalidBeanException {
		super(constBean, rel);
	}

   /**
    * New index
    * @param name Index name
    * @param rel Relation to which the index applies
    * @param fields Names of fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    */	
	public RelationIndex(String name, AbstractRelation rel, List<String> fields) 
				throws UnknownRefFieldException {
		super(name, rel, fields);
	}

   /**
    * New index
    * @param name Index name
    * @param rel Relation to which the index applies
    * @param fields Names of fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    */	
	public RelationIndex(String name, AbstractRelation rel, String[] fields) throws UnknownRefFieldException {
		super(name, rel, fields);
	}
	

	@Override
	public ScConstraintBean toBean() {		
		ScConstraintBean bean =  super.toBean();
		return bean;
	}
	
	

}
