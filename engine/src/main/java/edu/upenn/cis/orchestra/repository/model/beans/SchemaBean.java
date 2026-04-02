package edu.upenn.cis.orchestra.repository.model.beans;



import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;

//TODO: Implement InitializingBean? Pb: duplicates control with objects ...

public class SchemaBean 
{
   /** Schema id. Should be unique for a given peer, unicity not managed in this class */
   private String _schemaId;
   /** Schema description */
   private String _description;
   /** 
    * List of relations stored in this schema. 
    */
   protected List<ScRelationBean> _relations = new ArrayList<ScRelationBean>();
  
   
   public SchemaBean ()
   {
   }
   
	/**
	 * Had to create this method to allow copy of beans with same properties than our model beans, because 
	 * JaxB does not allow to replace the generated beans for client applications by our own beans. Event though they
	 * will have exactly the same getters and settes
	 * @param beanSameProperties Bean to copy in our standard beans
	 */   
   public SchemaBean (Object beanSameProperties)
   {
	   try
	   {
	   	   // Copy the basic properties
		   BeanUtils.copyProperties(beanSameProperties, this, new String[] {"relations"});
		   // Copy the list of relations
		   for (Object rel : (List) BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "relations").getReadMethod().invoke(beanSameProperties, new Object[]{}))
			   _relations.add (new ScRelationBean(rel));
		}
		catch (InvocationTargetException e)
		{
			//TODO: Log + terminate
			System.out.println ("RUNTIME ERROR: " + e.getMessage());
			e.printStackTrace();			
		}
		catch (IllegalAccessException e)
		{
			//TODO: Log + terminate
			System.out.println ("RUNTIME ERROR: " + e.getMessage());
			e.printStackTrace();			
		}		   
   }
   
   /* Not used anymore (using BeanUtils.copyProperties now)
   public SchemaBean (String schemaId, String description, List<ScRelationBean> relations)
   {
	   _schemaId = schemaId;
	   _description = description;
	   _relations = relations;
   }
   */

   /**
    * Getters and setters
    */	   
	public String getDescription() {
		return _description;
	}

	public void setDescription(String description) {
		this._description = description;
	}

	public List<ScRelationBean> getRelations() {
		return _relations;
	}

	public void setRelations(List<ScRelationBean> relations) {
		if (relations != null)
			this._relations = relations;
	}

	public String getSchemaId() {
		return _schemaId;
	}

	public void setSchemaId(String id) {
		_schemaId = id;
	}
	   

}
