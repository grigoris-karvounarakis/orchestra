package edu.upenn.cis.orchestra.datamodel.exceptions;

/**
 * Exception raised whenever an incomplete/incorrect bean is used to 
 * initialize a model class 
 * @author Olivier Biton
 *
 */
public class InvalidBeanException extends ModelException{
	public static final long serialVersionUID = 1L; 
	
	private Object _bean;
	
	public Object getBean() {
		return _bean;
	}

	public InvalidBeanException (Object bean, String errMsg)
	{
		super (errMsg + "(" + bean.getClass().getName() + ")");
		_bean = bean;
	}
}
