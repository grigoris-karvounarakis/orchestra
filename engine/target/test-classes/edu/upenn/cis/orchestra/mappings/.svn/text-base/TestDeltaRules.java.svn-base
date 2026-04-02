package edu.upenn.cis.orchestra.mappings;

import junit.framework.TestCase;

public class TestDeltaRules extends TestCase {
	public static String SCHEMAFILE = "classpath:edu/upenn/cis/orchestra/mappings/TJs-out.txt";

/*
//	public static String SCHEMAFILE = "classpath:edu/upenn/cis/orchestra/mappings/foo.schema";
	//public static String SCHEMAFILE = "classpath:edu/upenn/cis/orchestra/mappings/GregsTest.txt";
	//public static String SCHEMAFILE = "classpath:edu/upenn/cis/orchestra/mappings/TestOlivier.txt";
	
	public static void main(String[] args)
			throws FileNotFoundException, IOException, ParseException, RulesCycleException, SQLException
			
	{
		FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(SCHEMAFILE);
		OrchestraSystem system = dao.loadAllPeers();
		List<ScMapping> mappings = system.getAllSystemMappings(true);
		
		System.out.println ("Mappings: " + mappings.size());
		
		Calendar before = Calendar.getInstance();
//		System.out.println ("+++++++++++AFTER INVERSION++++++++++");
		List<Rule> rules = MappingsInversionMgt.inverseMappings(mappings);
//		for (Rule rule : rules)
//			System.out.println (rule.toString());

//		System.out.println ("+++++++++++AFTER COMPOSITION++++++++++");
		MappingsCompositionMgt.compose(rules);
//		for (Rule rule : rules)
//			System.out.println (rule.toString());
		
//		System.out.println ("+++++++++++AFTER DELTA++++++++++");
		// Cache data in the first peer/first schema...
		Peer cachePeer = system.getPeers().iterator().next();
		Schema cacheSchema = cachePeer.getSchemas().iterator().next();
		String cacheDbCatal = cacheSchema.getRelations().iterator().next().getDbCatalog();
		String cacheDbSchema = cacheSchema.getRelations().iterator().next().getDbSchema();
		
		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("TOTAL RULE MANIPULATION TIME: " + time + "msec");
        
		before = Calendar.getInstance();
			
		// Our algorithm
		new DeltaRules (rules, cachePeer, cacheSchema, cacheDbCatal, cacheDbSchema, system, false);
		
//		 DRed
//		new DeltaRules (rules, cachePeer, cacheSchema, cacheDbCatal, cacheDbSchema, system, true);
		
		after = Calendar.getInstance();
		time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("TOTAL DELTA TIME: " + time + "msec");
		
		
		/*
		System.out.println ("-------Edbs-----------");
		List<RelationContext> rels = delta.getEdbs();
		for (RelationContext rel : rels)
			System.out.println (rel.toString());
		System.out.println ("-------Idbs-----------");
		rels = delta.getIdbs();
		for (RelationContext rel : rels)
			System.out.println (rel.toString());
		System.out.println ("-------Mapping rules-----------");
		rules = delta.getMappingRules();
		for (Rule rule : rules)
			System.out.println (rule.toString());
		System.out.println ("-------Mapping projections-----------");
		rules = delta.getMappingsProjection ();
		for (Rule rule : rules)
			System.out.println (rule.toString());
		System.out.println ("-------Insertion rules-----------");
		
		System.out.println(delta.getInsertionRules().toString());
		
		System.out.println ("-------Deletion rules-----------");
	
		
		System.out.println(delta.getDeletionRules().toString());
		/
		//TODO: Complete test (fill the flat file / hard code res test)		
	//}


	// Calls DRed
	public void testDeltaRules ()
	//public static void main(String[] args)
			throws FileNotFoundException, IOException, ParseException, RulesCycleException, SQLException
	{
		FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO (SCHEMAFILE);
		OrchestraSystem system = dao.loadAllPeers();
		List<ScMapping> mappings = system.getAllSystemMappings(true);
		
		System.out.println ("Mappings: " + mappings.size());
		
		Calendar before = Calendar.getInstance();
		System.out.println ("+++++++++++AFTER INVERSION++++++++++");
		List<Rule> rules = MappingsInversionMgt.inverseMappings(mappings);
		for (Rule rule : rules)
			System.out.println (rule.toString());

		System.out.println ("+++++++++++AFTER COMPOSITION++++++++++");
		MappingsCompositionMgt.compose(rules);
		for (Rule rule : rules)
			System.out.println (rule.toString());
		
		System.out.println ("+++++++++++AFTER DELTA++++++++++");
		// Cache data in the first peer/first schema...
		Peer cachePeer = system.getPeers().iterator().next();
		Schema cacheSchema = cachePeer.getSchemas().iterator().next();
		String cacheDbCatal = cacheSchema.getRelations().iterator().next().getDbCatalog();
		String cacheDbSchema = cacheSchema.getRelations().iterator().next().getDbSchema();
		
		Calendar after = Calendar.getInstance();
		long time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("TOTAL RULE MANIPULATION TIME: " + time + "msec");
        
		before = Calendar.getInstance();
				
		// DRed
		DeltaRules delta = new DeltaRules (rules, cachePeer, cacheSchema, cacheDbCatal, cacheDbSchema, system, true);
		after = Calendar.getInstance();
		time = after.getTimeInMillis() - before.getTimeInMillis();
        System.out.println("TOTAL DELTA TIME: " + time + "msec");
		
		
		/*
		System.out.println ("-------Edbs-----------");
		List<RelationContext> rels = delta.getEdbs();
		for (RelationContext rel : rels)
			System.out.println (rel.toString());
		System.out.println ("-------Idbs-----------");
		rels = delta.getIdbs();
		for (RelationContext rel : rels)
			System.out.println (rel.toString());
		System.out.println ("-------Mapping rules-----------");
		rules = delta.getMappingRules();
		for (Rule rule : rules)
			System.out.println (rule.toString());
		System.out.println ("-------Mapping projections-----------");
		rules = delta.getMappingsProjection ();
		for (Rule rule : rules)
			System.out.println (rule.toString());
		
		/
        //System.out.println ("-------Insertion rules-----------");
		
		//System.out.println(delta.getInsertionRules().toString());
		
		//System.out.println ("-------Deletion rules-----------");
	
		 
		//System.out.println(delta.getDeletionRules().toString());
		//TODO: Complete test (fill the flat file / hard code res test)		
	}*/

}
