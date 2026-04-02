package edu.upenn.cis.orchestra.repository.dao.flatfile;



import java.io.FileReader;

import junit.framework.TestCase;

import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.FlatRepository;

public class TestFlatFile extends TestCase {

	@Test
	public void testToConvertToActualTest () throws Exception
	{
		DefaultResourceLoader loader = new DefaultResourceLoader ();
		
		
		FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO ("classpath:edu/upenn/cis/orchestra/repository/dao/flatfile/testTJ.txt");
		OrchestraSystem system = dao.loadAllPeers();
		System.out.println (system.toString());


		Resource ress = loader.getResource("classpath:edu/upenn/cis/orchestra/repository/dao/flatfile/catalExample.txt");
		FlatRepository repos = new FlatRepository (new FileReader(ress.getFile())); 
		system = repos.orchestraSystem(); 
		System.out.println (system.toString());
			
	}

}
