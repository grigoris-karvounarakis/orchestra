package edu.upenn.cis.orchestra.mappings;

import java.util.List;

import junit.framework.TestCase;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Mapping;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

public class TestMappingsComposition extends TestCase {

	
	public void testBasicComposition ()
						throws Exception
	{
		FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
		OrchestraSystem system = dao.loadAllPeers();
		List<Mapping> mappings = system.getAllSystemMappings(true);
		
		System.out.println ("Mappings: " + mappings.size());
		
		System.out.println ("+++++++++++AFTER INVERSION++++++++++");
		List<Rule> rules = MappingsInversionMgt.inverseMappings(mappings, system.getMappingDb());
		for (Rule rule : rules)
			System.out.println (rule.toString() + "\n");

		System.out.println ("+++++++++++AFTER COMPOSITION++++++++++");
		MappingsCompositionMgt.composeRules(rules);
		for (Rule rule : rules)
			System.out.println (rule.toString() + "\n");
		
		
		//TODO: Complete test (fill the flat file / hard code res test)
			
	}
}