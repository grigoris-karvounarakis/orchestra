package edu.upenn.cis.orchestra.repository.model.beans;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;


public class ScConstraintBean {

	/**
	 * Constraint name
	 */
	private String _name;
	/**
	 * Fields (names) on which the constraint will apply
	 * We use the names and not fields objects because IBATIS does not 
	 * deal yet with circular dependencies.
	 * Moreover we might be able to use the beans in web services 
	 * to reduce the complexity of the XML messages produced by 
	 * JAVA-WS
	 */
	private List<String> _fields = new ArrayList<String> ();
	
	/**
	 * Field type as defined in the TableSchema class
	 * @see AbstractRelation
	 */
	private String _type;
	
	/**
	 * Number of unique values, used only for Indexes
	 *
	 */
	private int _statsNbUniqueVals = -1;
	
	

	public ScConstraintBean ()
	{
		super ();
	}
	
	/**
	 * Had to create this method to allow copy of beans with same properties than our model beans, because 
	 * JaxB does not allow to replace the generated beans for client applications by our own beans. Event though they
	 * will have exactly the same getters and settes
	 * @param beanSameProperties Bean to copy in our standard beans
	 */	
	public ScConstraintBean (Object beanSameProperties)
	{
		super ();
		BeanUtils.copyProperties(beanSameProperties, this);
	}
	
	
	public int getStatsNbUniqueVals() {
		return _statsNbUniqueVals;
	}

	public void setStatsNbUniqueVals(int nbUniqueVals) {
		_statsNbUniqueVals = nbUniqueVals;
	}
	
	
	public List<String> getFields() {
		return _fields;
	}
	public void setFields(List<String> fields) {
		if (fields != null)
			this._fields = fields;
	}
	public String getName() {
		return _name;
	}
	public void setName(String name) {
		this._name = name;
	}
	
	/**
	 * Get the constraint type, as defined in the relation class
	 * @return Constraint type
	 * @see AbstractRelation
	 */
	public String getType() {
		return _type;
	}

	/**
	 * Set the constraint type, as defined in the relation class
	 * @param type Constraint type
	 * @see AbstractRelation
	 */
	public void setType(String type) {
		this._type = type;
	}
	
	
}
