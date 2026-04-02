package edu.upenn.cis.orchestra.repository.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;

public class ScMappingAtomBean {

	private String _peerId;
	private String _schemaId;
	private String _relationId;
	private int _atomPosition;
	private List<ScMappingAtomValueBean> _values=null;
	//TODO: Atom type is not stored in the bean. To add if necessary for some component!
	
	public ScMappingAtomBean ()
	{
		super ();
	}
	
	public ScMappingAtomBean (Object beanSameProperties)
	{
		super ();
		BeanUtils.copyProperties(beanSameProperties, this, new String[]{"values"});
		try
		{
			_values = new ArrayList<ScMappingAtomValueBean> ();
			// Copy the values
			for (Object value : (List) BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "values").getReadMethod().invoke(beanSameProperties, new Object[]{}))
			{
				if (BeanUtils.getPropertyDescriptor(value.getClass(), "parameters") != null)
					_values.add (new ScMappingAtomValSkolemBean(value));
				else
					_values.add (new ScMappingAtomValueBean(value));
			}
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

	public List<ScMappingAtomValueBean> getValues() {
		return _values;
	}

	public void setValues(List<ScMappingAtomValueBean> values) {
		_values = values;
	}

	public String getPeerId() {
		return _peerId;
	}

	public void setPeerId(String peerId) {
		this._peerId = peerId;
	}

	public String getRelationId() {
		return _relationId;
	}

	public void setRelationId(String relationId) {
		this._relationId = relationId;
	}

	public String getSchemaId() {
		return _schemaId;
	}

	public void setSchemaId(String schemaId) {
		this._schemaId = schemaId;
	}

	public int getAtomPosition() {
		return _atomPosition;
	}

	public void setAtomPosition(int atomPosition) {
		this._atomPosition = atomPosition;
	}
	
	
	
}
