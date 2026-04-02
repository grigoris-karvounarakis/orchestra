package edu.upenn.cis.orchestra.repository.model.beans;

import org.springframework.beans.BeanUtils;

public class ScMappingAtomValueBean 
{
	
	private String _type;
	private String _id;

	public ScMappingAtomValueBean ()
	{
		super ();
	}
	
	public ScMappingAtomValueBean (Object beanSameProperties)
	{
		this();
		BeanUtils.copyProperties(beanSameProperties, this);
	}
	
	
	public String getType() {
		return _type;
	}

	public void setType(String type) {
		this._type = type;
	}

	public String getId() {
		return _id;
	}

	public void setId(String id) {
		this._id = id;
	}
	
	

}
