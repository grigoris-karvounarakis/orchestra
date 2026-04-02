//Source file: D:\\PENN\\SHARQ\\API\\SCHEMAAPI\\src\\main\\java\\edu\\upenn\\cis\\orchestra\\repository\\model\\ScConstraint.java

package edu.upenn.cis.orchestra.datamodel;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.beans.BeanUtils;

import edu.upenn.cis.orchestra.datamodel.exceptions.InvalidBeanException;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnknownRefFieldException;
import edu.upenn.cis.orchestra.repository.model.beans.ScConstraintBean;

/****************************************************************
 * Abstract class used as a basis for relations constraints
 * It only has a name and a list of fields.
 * @author Olivier Biton
 *****************************************************************
 */
public abstract class IntegrityConstraint implements Serializable
{
	public static final long serialVersionUID = 1L;
	
   /**
    * Constraint name
    */
   protected final String _name;
   /**
    * Fields on which the constraint will apply. An immutable list
    */
   protected final List<RelationField> _fields;

   /**
    * Relation to which the constraint applies
    */
   protected final String _relName;
   
   /**
    * New constraint
    * @param name Constraint name
    * @param rel Relation to which the constraints applied
    * @param fields Fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    * @roseuid 449AEAF80177
    */
   public IntegrityConstraint(String name, AbstractRelation rel, List<String> fields) 
   				throws UnknownRefFieldException
   {
	   this (name, rel, fields.toArray(new String[0]));
   }
   
   /**
    * New constraint
    * @param name Constraint name
    * @param rel Relation to which the constraints applied
    * @param fields Fields on which it applies
    * @throws UnknownRefFieldException If a field in the list does not exist in the relation
    */
   public IntegrityConstraint(String name, AbstractRelation rel, String[] fields) 
   				throws UnknownRefFieldException
   {
	   if (name == null || name.length() == 0) {
		   throw new IllegalArgumentException("Cannot create an integrity constraint with no name");
	   }
	   _name = name;
	   _relName = rel.getName();
	   
	   List<RelationField> fieldsList = new ArrayList<RelationField>(fields.length);
	   for (String field : fields)
	   {
		   if (rel.getField(field)==null)
			   throw new UnknownRefFieldException ("Constraint " + name + " references unknown field " + rel.getName() + "." + field);
		   else
			   fieldsList.add (rel.getField(field));
	   }
	   _fields = Collections.unmodifiableList(fieldsList);
   }
   
   /**
    * New constraint, from it's bean representation.
    * Sub classes will have to check compatibility with the bean type
    * @param constBean Bean describing the constraint
    * @param rel Relation to which the constraint is related
    * @throws InvalidBeanException If a field does not exist in the relation
    */
   public IntegrityConstraint (ScConstraintBean constBean, AbstractRelation rel) 
   				throws InvalidBeanException
   {
	   _name = constBean.getName();
	   _relName = rel.getName();
	   List<RelationField> fieldsList = new ArrayList<RelationField>();
	   for (String fieldName : constBean.getFields())
	   {
		   if (rel.getField(fieldName)==null)
			   throw new InvalidBeanException (constBean, "Field " + fieldName 
					   										+ "specified in constraint " 
					   										+ constBean.getName() 
					   										+ " does not exist in relation " 
					   										+ rel.getName());
		   fieldsList.add (rel.getField(fieldName));
	   }
	   _fields = Collections.unmodifiableList(fieldsList);
   }
      
   public String getRelation() {
	   return _relName;
   }
   
   /**
    * Get the list of fields on which the constraint applies
    * @return fields
    * @roseuid 449AE9D60399
    */
   public List<RelationField> getFields() 
   {
    return _fields;
   }
   

   
   /**
    * Get the constraint's name
    * @return Constraint's name
    * @roseuid 44AD2D9A0399
    */
   public String getName() 
   {
    return _name;
   }
   

   /**
    * Returns a description of the constraint, conforms to the 
    * flat file format defined in <code>RepositoryDAO</code>
    * @return Constraint's description
    */
   public String toString ()
   {
	   String description;
	   
	   description = getName() + "(";
	   boolean firstField = true;
	   for (RelationField fld : getFields())
	   {
		   description += (firstField?"":", ") + fld.getName();
		   firstField = false;
	   }
	   description += ")";
	   
	   return description;
   }
   
   

   
   
   /**
    * Creates a bean representation of this object.<BR>
    * Subclasses: override only setBeanProperties
    * @return The bean
    * @see IntegrityConstraint#setBeanProperties(ScConstraintBean)
    */
   public ScConstraintBean toBean ()
   {
	   ScConstraintBean bean = new ScConstraintBean ();
	   setBeanProperties(bean);
	   return bean;
   }
   
   /**
    * Sets bean properties to reflect this constraint's properties. <BR>
    * To override in subclasses (set the constraint type)
    * @param bean the bean for which to set properties
    * @see IntegrityConstraint#toBean()
    */
   protected void setBeanProperties (ScConstraintBean bean)
   {   
	   setBeanProperties(bean, null);
   }
   
   /**
    * Sets bean properties to reflect this constraint's properties. <BR>
    * To override in subclasses (set the constraint type)
    * @param bean the bean for which to set properties
    * @param ignoreProp Properties to ignore when copying bean properties, can be null
    * @see IntegrityConstraint#toBean()
    */
   protected void setBeanProperties (ScConstraintBean bean, List<String> ignoreProp)
   {
	   /*bean.setName(getName());*/

	   // Copy the simple bean properties 
	   String[] ignore = new String[(ignoreProp==null?0:ignoreProp.size())+2];
	   int i = 0;
	   if (ignoreProp != null)
	   	   for (String prop : ignoreProp)
	   		   ignore[i++] = prop;
	   ignore[i++] = new String("fields");
	   ignore[i++] = new String("type");

	   
	   BeanUtils.copyProperties(this, bean, ignore);
	   
	   // Copy the list of fields
	   List<String> fields = new ArrayList<String> (getFields().size());
	   for (RelationField fld : getFields())
		   fields.add (fld.getName());
	   bean.setFields(fields);
   }
   
   protected boolean constraintEquals(IntegrityConstraint c) {
	   return _name.equals(c._name) && _relName.equals(c._relName) && _fields.equals(c._fields); 
   }
   
   protected int constraintHashCode() {
	   return _name.hashCode() + 31 *_relName.hashCode() + 61 * _fields.hashCode();
   }
}
