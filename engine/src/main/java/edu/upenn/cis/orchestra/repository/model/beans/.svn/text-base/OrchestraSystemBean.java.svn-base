/**
 * This package contains all the beans necessary to represent the Repository classes.
 * These beans can be used to communicate with DAO, web services... 
 */
package edu.upenn.cis.orchestra.repository.model.beans;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.BeanUtils;

public class OrchestraSystemBean {

	private List<PeerBean> _peers;

	
	
	public OrchestraSystemBean ()
	{
		super ();
		_peers = new ArrayList<PeerBean> ();
	}
	
	/**
	 * Had to create this method to allow copy of beans with same properties than our model beans, because 
	 * JaxB does not allow to replace the generated beans for client applications by our own beans. Event though they
	 * will have exactly the same getters and settes
	 * @param beanSameProperties Bean to copy in our standard beans
	 */	
	public OrchestraSystemBean (Object beanSameProperties)
	{
		this();
		BeanUtils.copyProperties(beanSameProperties, this, new String[] {"peers"});
		try
		{
			for (Object peerBean : (List) (BeanUtils.getPropertyDescriptor(beanSameProperties.getClass(), "peers").getReadMethod().invoke(beanSameProperties, new Object[] {})))
				_peers.add (new PeerBean(peerBean));
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
	
	public List<PeerBean> getPeers() {
		return _peers;
	}

	public void setPeers(List<PeerBean> peers) {
		this._peers = peers;
	}
	
	
	
}
