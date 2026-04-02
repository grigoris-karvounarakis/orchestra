package edu.upenn.cis.orchestra;

import java.util.List;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.dbms.TukwilaDb;
import edu.upenn.cis.orchestra.exchange.tukwila.TukwilaEngine;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;

/**
 * Mainline to run the Tukwila Orchestra mapping application
 * 
 * @author tjgreen
 *
 */
public class TukwilaMap {
	public static void main(String[] args) throws Exception {
		Config.parseCommandLine(args);
		Config.dumpParams(System.out);

		FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(Config.getSchemaFile());
		OrchestraSystem system = dao.loadAllPeers();
		List<String> tables = BasicEngine.getNamesOfAllTables(system, false, false, true);
		TukwilaDb db = new TukwilaDb(tables, system.getAllSchemas());
		TukwilaEngine engine = new TukwilaEngine(db, //null, 
				system, true);

//		Debug.println("TABLES:");
//		for (String t: tables)
//			Debug.println(t + "\t");
//		Debug.println("");
//
//		Debug.println("MAPPINGS:");
//		List<ScMapping> mappings = system.getAllSystemMappings(true);
//		for (ScMapping s: mappings)
//			Debug.println(s.toString());
//
//		Debug.println("INVERSE MAPPINGS:");
//		List<Rule> ruleSet = 
		engine.computeTranslationRules();

//		for (Rule r: ruleSet)
//			Debug.println(r.toString());

		//DeltaRules ourRules = 
		engine.computeDeltaRules(); // (false, true) = our algorithm

//		Debug.println("TRANSLATED SCHEMA"); 
//		List<RelationContext> lc = engine.getMappingRelations();
//		for (RelationContext c : lc) {
//			Debug.println(c.getRelation().toString());
//		}
//		
//		if (Config.DO_INSERT) {
//			Debug.println("INSERTION RULES:");
//			List<DatalogSequence> insRules = engine.getInsertionRules();
//			insRules.get(0).printString();
//			insRules.get(1).printString();
//			insRules.get(2).printString();
//		}
//
//		if (Config.DO_DELETE) {
//			Debug.println("DELETION RULES:");
//			List<DatalogSequence> delRules = engine.getDeletionRules();
//			delRules.get(0).printString();
//			delRules.get(1).printString();
//			delRules.get(2).printString();
//		}
		
		List<String> newTables = BasicEngine.getNamesOfAllTablesFromDeltas(/*ourRules,*/ system, true, true, true);
		db.setAllTables(newTables);

		Debug.println("TEST FILE: " + Config.getWorkloadPrefix());
		int recno = 0;
		Peer p = system.getPeers().iterator().next();
		for (int i = 1; i <= Config.getInteger("runs"); i++) {
			Debug.println("RUN " + i + ":");
			if (Config.getMigrate()) {
				engine.migrate();
			}
			if (Config.getBoolean("import")) {
				engine.importUpdates(null);
			}
			if (Config.getApply()) {
				engine.mapUpdates(recno, ++recno, p, true);
			}
		}
	}
}
