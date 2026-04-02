package edu.upenn.cis.orchestra.repository.dao.flatfile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.repository.dao.RepositorySchemaDAOWithMemoryCache;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.FlatRepository;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.ParseException;


/**
 * DAO implementation using a flat file to load the system in memory.
 * TODO: Add an option to constructor to write the new system in a file when unloaded 
 * 
 * @author Olivier Biton
 *
 */
public class FlatFileRepositoryDAO extends RepositorySchemaDAOWithMemoryCache {

	private Log _log = LogFactory.getLog(getClass());
	
//	public FlatFileRepositoryDAO (FileReader reader)
//			throws ParseException
//	{
//		loadFromReader(reader);
//	}

	
	public FlatFileRepositoryDAO (String filePath)
			throws Exception
	{
		DefaultResourceLoader loader = new DefaultResourceLoader();
		File file = new File(filePath);
		if (!file.exists()) {
			Resource ress = loader.getResource(filePath);
			file = ress.getFile();
		}
		if (Config.getXMLFormat()) {
			FileInputStream stream = new FileInputStream(file);
			setSystem(OrchestraSystem.deserialize(stream));
		} else {
			FileReader reader = new FileReader(file);
			loadFromReader(reader);
		}
	}

	private void loadFromReader (FileReader reader)
			throws ParseException
	{
		FlatRepository repos = new FlatRepository (reader); 
		setSystem(repos.orchestraSystem());
		
		// Use code guard to avoid computing toString if not necessary
		if (_log.isInfoEnabled())
			_log.info("Loaded system:\n" + _inMemoryRepos.toString());
	}	
}
