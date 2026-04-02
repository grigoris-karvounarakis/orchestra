package edu.upenn.cis.orchestra.repository.model.beans;

import org.springframework.beans.BeanUtils;

public class ScFieldBean 
{
	/** Field name */
	private String _name;
	/** Field description */
	private String _description;
	
    /** Field type as defined in the database (specific to the 
     * RDBMS while <code>_type</code> is generic in Java)
     */
    private String _dbType;
    
    /**
     * Field size. For char or date types this is the maximum number of 
     * characters, for numeric or decimal types this is precision
     */
    private int _dbSize;
	   
	/**
	 * Field type, as defined in the JDBC API
	 * @see java.sql.Types
	 */
	private int _type;
	/** True if the field can contain null values */
	private boolean _isNullable;

   
	public ScFieldBean ()
	{
		super ();		
	}   
	
	/**
	 * Had to create this method to allow copy of beans with same properties than our model beans, because 
	 * JaxB does not allow to replace the generated beans for client applications by our own beans. Event though they
	 * will have exactly the same getters and settes
	 * @param beanSameProperties Bean to copy in our standard beans
	 */	
	public ScFieldBean (Object samePropertiesBean)
	{
		super ();
		BeanUtils.copyProperties(samePropertiesBean, this);
	}
	
	public String getDbType() {
		return _dbType;
	}
	
	public void setDbType(String dbType) {
		_dbType = dbType;
	}
   
	public String getDescription() {
		return _description;
	}
	public void setDescription(String description) {
		this._description = description;
	}
	public boolean isNullable() {
		return _isNullable;
	}
	public void setNullable(boolean isNullable) {
		_isNullable = isNullable;
	}
	public String getName() {
		return _name;
	}
	public void setName(String name) {
		this._name = name;
	}
	public int getType() {
		return _type;
	}
	public void setType(int type) {
		this._type = type;
	}

	public int getDbSize() {
		return _dbSize;
	}

	public void setDbSize(int dbSize) {
		this._dbSize = dbSize;
	}
   
   
}
