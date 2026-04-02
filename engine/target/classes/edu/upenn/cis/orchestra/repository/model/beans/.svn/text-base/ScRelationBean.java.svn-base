package edu.upenn.cis.orchestra.repository.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;


import org.springframework.beans.BeanUtils;


public class ScRelationBean {
	/** Catalog used for the relation in the source database, 
	 * might be null */
	private String _dbCatalog=null;
	/** Schema used for the relation in the source database, 
	 * might be null */
	private String _dbSchema=null;
	/** Relation physical name in the database. Can be equal to the 
	 * "Orchestra name" (_name) but the Orchestra name has to be 
	 * unique for a given Orchestra schema
	 */
	private String _dbRelName=null;
	/** Relation name */
	private String _name=null;
	/** Relation description */
	private String _description=null;
	/** Relation fields */
	protected List<ScFieldBean> _fields = new ArrayList<ScFieldBean> ();
	/** Is this a virtual or materialized relation */
	private boolean _materialized;
	/** A relation may have local data in name_L relation **/
	protected boolean _hasLocalData;
	/** Statistics: number of rows in this relation */
	private int _statNbRows=-1;
	   
	/*** Constraints applicable directly to the relation (no external dependencies). */
	protected List<ScConstraintBean> _directConstraints = new ArrayList<ScConstraintBean> ();
	/** Foreign keys */
	protected List<ScForeignKeyBean> _foreignKeys = new ArrayList<ScForeignKeyBean> ();
	
	public ScRelationBean ()
	{
		
	}
	
	/**
	 * Had to create this method to allow copy of beans with same properties than our model beans, because 
	 * JaxB does not allow to replace the generated beans for client applications by our own beans. Event though they
	 * will have exactly the same getters and settes
	 * @param beanSameProperties Bean to copy in our standard beans
	 */
	public ScRelationBean (Object beanSameProperties)
	{
		try
		{
			// Copy simple properties
			BeanUtils.copyProperties(beanSameProperties, this, new String[] {"fields", "directConstraints", "foreignKeys"});
			// Copy fields
			for (Object field : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "fields").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				_fields.add (new ScFieldBean(field));
			// Copy direct constraints
			for (Object cst : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "directConstraints").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				_directConstraints.add (new ScConstraintBean(cst));
			// Copy foreign keys
			for (Object cst : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "foreignKeys").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				_foreignKeys.add (new ScForeignKeyBean(cst));
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
	
	
	public List<ScConstraintBean> getDirectConstraints() {
		return _directConstraints;
	}

	public void setDirectConstraints(List<ScConstraintBean> constraints) {
		if (constraints != null)
			this._directConstraints = constraints;
	}
	
	public List<ScForeignKeyBean> getForeignKeys ()
	{
		return _foreignKeys;
	}
	
	public void setForeignKeys (List<ScForeignKeyBean> fks)
	{
		if (fks != null)
			_foreignKeys = fks;
	}
	

	public String getDbCatalog() {
		return _dbCatalog;
	}

	public void setDbCatalog(String catalog) {
		_dbCatalog = catalog;
	}

	public String getDbSchema() {
		return _dbSchema;
	}

	public void setDbSchema(String schema) {
		_dbSchema = schema;
	}

	public String getDescription() {
		return _description;
	}

	public void setDescription(String description) {
		this._description = description;
	}

	public List<ScFieldBean> getFields() {
		return _fields;
	}

	public void setFields(List<ScFieldBean> fields) {
		if (fields != null)
			this._fields = fields;
	}

	public String getName() {
		return _name;
	}

	public void setName(String name) {
		this._name = name;
	}

	public String getDbRelName() {
		return _dbRelName;
	}

	public void setDbRelName(String name) {
		_dbRelName = name;
	}

	public boolean isMaterialized() {
		return _materialized;
	}
	
	public boolean hasLocalData() {
		return _hasLocalData;
	}

	public void setMaterialized(boolean materialized) {
		this._materialized = materialized;
	}

	public int getStatNbRows() {
		return _statNbRows;
	}

	public void setStatNbRows(int nbRows) {
		_statNbRows = nbRows;
	}
	
}
