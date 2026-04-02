package edu.upenn.cis.orchestra.repository.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;


import org.springframework.beans.BeanUtils;

public class ScMappingAtomValSkolemBean extends ScMappingAtomValueBean {

	private List<ScMappingAtomValueBean> _parameters = new ArrayList<ScMappingAtomValueBean> ();
	
	public ScMappingAtomValSkolemBean ()
	{
		super ();
		setType("S");
	}
	
	public ScMappingAtomValSkolemBean (Object beanSameProperties)
	{
		super (beanSameProperties);
		setType("S");
		
		
		try
		{
			// Copy parameters
			for (Object param : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "parameters").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				if (BeanUtils.getPropertyDescriptor(param.getClass(), "parameters")!=null)
					_parameters.add (new ScMappingAtomValSkolemBean(param));
				else
					_parameters.add (new ScMappingAtomValueBean(param));
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

	public List<ScMappingAtomValueBean> getParameters() {
		return _parameters;
	}

	public void setParameters(List<ScMappingAtomValueBean> parameters) {
		this._parameters = parameters;
	}
	

	
}
