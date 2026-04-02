package edu.upenn.cis.orchestra.gui;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAO;

public class GUIObjectsFactory {

	private static GUIObjectsFactory _singleton=null;
	
	private final ApplicationContext _ctx = new ClassPathXmlApplicationContext ("edu/upenn/cis/orchestra/gui/SpringConfig.xml");
	
	private RepositorySchemaDAO _reposDAO = null;
	
	
	
	/**
	 * This is a singleton, use getInstance() instead
	 *
	 */ 
	private GUIObjectsFactory ()
	{		
	}
	
	/**
	 * Get the factory singleton
	 * @return
	 */
	public static GUIObjectsFactory getInstance ()
	{
		if (_singleton == null)
			_singleton = new GUIObjectsFactory ();
		return _singleton;
	}
	
	/**
	 * Get the default repository DAO.
	 * On first call this is loaded using Spring, then the same object is cached and returned.
	 * @return Default repository DAO object
	 */
	public RepositorySchemaDAO getRepositoryDAO ()
	{
		if (_reposDAO == null)
			_reposDAO = (RepositorySchemaDAO) _ctx.getBean("defaultReposDAO");
		return _reposDAO;
	}
	
	
}
