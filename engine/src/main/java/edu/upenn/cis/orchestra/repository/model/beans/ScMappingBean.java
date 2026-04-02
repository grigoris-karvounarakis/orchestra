package edu.upenn.cis.orchestra.repository.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;


public class ScMappingBean {

	private String _id;
	private String _description;
	private boolean _materialized;
	private int _trustRank;
	private List<ScMappingAtomBean> _head = null;
	private List<ScMappingAtomBean> _body = null;
	
	public ScMappingBean ()
	{
		super ();
	}
	
	public ScMappingBean (Object beanSameProperties)
	{
		try
		{
			BeanUtils.copyProperties(beanSameProperties, this, new String[]{"head", "body"});
			_head = new ArrayList<ScMappingAtomBean> ();
			_body = new ArrayList<ScMappingAtomBean> ();
			for (Object atom : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "head").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				_head.add (new ScMappingAtomBean(atom));
			for (Object atom : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "body").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				_body.add (new ScMappingAtomBean(atom));
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

	public String getDescription() {
		return _description;
	}

	public void setDescription(String description) {
		this._description = description;
	}

	public String getId() {
		return _id;
	}

	public void setId(String id) {
		this._id = id;
	}

	public List<ScMappingAtomBean> getHead() {
		return _head;
	}

	public void setHead(List<ScMappingAtomBean> atoms) {
		this._head = atoms;
	}

	public List<ScMappingAtomBean> getBody() {
		return _body;
	}

	public void setBody(List<ScMappingAtomBean> atoms) {
		this._body = atoms;
	}

	public boolean isMaterialized() {
		return _materialized;
	}

	public void setMaterialized(boolean materialized) {
		this._materialized = materialized;
	}

	public int getTrustRank() {
		return _trustRank;
	}

	public void setTrustRank(int trustRank) {
		this._trustRank = trustRank;
	}
	
}
