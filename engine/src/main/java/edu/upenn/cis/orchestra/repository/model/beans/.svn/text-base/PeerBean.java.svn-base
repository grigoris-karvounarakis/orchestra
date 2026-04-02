package edu.upenn.cis.orchestra.repository.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.ArrayList;

import org.springframework.beans.BeanUtils;



public class PeerBean  {
	/** Peer id, should be unique for the whole system */
	private String _id;
	/** Peer address, depending on the implementation. Could be IP:Port... */
	private String _address;
	/** Peer description, for final users */
	private String _description;
	/** Schemas defined for this peer */
	private List<SchemaBean> _schemas;
	
	//TODO:LOW Check if the schemas used in the head are schemas from this peer??

	/** Mappings defined by this peer */
	private List<ScMappingBean> _mappings;
	
	
	public PeerBean ()
	{
		
	}

	/**
	 * Had to create this method to allow copy of beans with same properties than our model beans, because 
	 * JaxB does not allow to replace the generated beans for client applications by our own beans. Event though they
	 * will have exactly the same getters and settes
	 * @param beanSameProperties Bean to copy in our standard beans
	 */	
	public PeerBean  (Object beanSameProperties)
	{
		this();
		_schemas =  new ArrayList<SchemaBean>();
		_mappings = new ArrayList<ScMappingBean> ();
		
		try
		{
			// Copy the basic properties
			BeanUtils.copyProperties(beanSameProperties, this, new String[] {"schemas", "mappings"});
			// Copy the schemas
			for (Object schema : (List) BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "schemas").getReadMethod().invoke(beanSameProperties, new Object[]{}))
				_schemas.add (new SchemaBean(schema));
			// Copy the mappings
			for (Object mapping : (List) BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "mappings").getReadMethod().invoke(beanSameProperties, new Object[]{}))
				_mappings.add (new ScMappingBean(mapping));
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

	public String getAddress() {
		return _address;
	}

	public void setAddress(String address) {
		this._address = address;
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

	public List<SchemaBean> getSchemas() {
		return _schemas;
	}

	public void setSchemas(List<SchemaBean> schemas) {
		this._schemas = schemas;
	}

	public List<ScMappingBean> getMappings() {
		return _mappings;
	}

	public void setMappings(List<ScMappingBean> mappings) {
		this._mappings = mappings;
	}
	
	
	public List<String> getPeersIdsDependencies ()
	{
		List<String> res = new ArrayList<String> (); 
		for (ScMappingBean mapp : getMappings())
		{
			for (ScMappingAtomBean atom : mapp.getHead())
				if (!res.contains(atom.getPeerId()))
					res.add (atom.getPeerId());
			for (ScMappingAtomBean atom : mapp.getBody())
				if (!res.contains(atom.getPeerId()))
					res.add (atom.getPeerId());
		}
		return res;
	}
}
