package edu.upenn.cis.orchestra.mappings;

import java.io.IOException;
import java.util.List;

import junit.framework.TestCase;

import org.xml.sax.SAXException;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.exceptions.UnsupportedTypeException;
import edu.upenn.cis.orchestra.dbms.SqlDb;
import edu.upenn.cis.orchestra.deltaRules.DeletionDeltaRuleGen;
import edu.upenn.cis.orchestra.deltaRules.InsertionDeltaRuleGen;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.exchange.IEngine;
import edu.upenn.cis.orchestra.exchange.sql.SqlEngine;
import edu.upenn.cis.orchestra.repository.dao.flatfile.FlatFileRepositoryDAO;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.ParseException;

public class TestSqlEngine extends TestCase {
	public void testMigrateDB () throws Exception
	{
		try {
			FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = BasicEngine.getNamesOfAllTables(dao.loadAllPeers(), true, true, true);
			SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);

			IEngine tcd = new SqlEngine(d, //null, 
					system, true);

			tcd.migrate();
		} catch (IOException i) {
			System.err.println("I/O error");
			i.printStackTrace();
			fail();
		} catch (ParseException p) {
			System.err.println("Parse error");
			p.printStackTrace();
			fail();
		} catch (java.sql.SQLException s) {
			System.err.println("SQL error");
			s.printStackTrace();
			fail();
		}
	}
	
	public void testInverseRules() throws UnsupportedTypeException {
		try {
			FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = BasicEngine.getNamesOfAllTables(dao.loadAllPeers(), true, true, true);
			SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);
			
			IEngine tcd = new SqlEngine(d, //null, 
					system, true);
			
			List<Rule> ruleSet = tcd.computeTranslationRules();
			System.out.println(ruleSet.toString());
		} catch (IOException i) {
			System.err.println("I/O error");
			i.printStackTrace();
			fail();
		} catch (ParseException p) {
			System.err.println("Parse error");
			p.printStackTrace();
			fail();
		} catch (SAXException e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		}
	}

	public void testInsDeltaRules() throws UnsupportedTypeException {
		try {
			FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = BasicEngine.getNamesOfAllTables(dao.loadAllPeers(), true, true, true);
			SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);
			IEngine tcd = new SqlEngine(d, //null, 
					system, true);
			
			tcd.computeTranslationRules();

			tcd.computeDeltaRules();
			new InsertionDeltaRuleGen(system);
		} catch (IOException i) {
			System.err.println("I/O error");
			i.printStackTrace();
			fail();
		} catch (ParseException p) {
			System.err.println("Parse error");
			p.printStackTrace();
			fail();
		} catch (SAXException e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		}
	}
	
	public void testDeletionDeltaRules() throws UnsupportedTypeException {
		try {
			FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = BasicEngine.getNamesOfAllTables(dao.loadAllPeers(), true, true, true);
			SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);
			IEngine tcd = new SqlEngine(d, //null, 
					system, true);
			
			tcd.computeTranslationRules();

			//DeltaRules dr = 
			tcd.computeDeltaRules();
			new DeletionDeltaRuleGen(system);
			
		} catch (IOException i) {
			System.err.println("I/O error");
			i.printStackTrace();
			fail();
		} catch (ParseException p) {
			System.err.println("Parse error");
			p.printStackTrace();
			fail();
		} catch (SAXException e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		}
	}

	public void testDeletionDredRules() throws UnsupportedTypeException {
		try {
			FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = BasicEngine.getNamesOfAllTables(dao.loadAllPeers(), true, true, true);
			SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);
			IEngine tcd = new SqlEngine(d, //null, 
					system, true);
			
			tcd.computeTranslationRules();

			//DeltaRules dr = 
			tcd.computeDeltaRules();
//	true/true = DRed w provenance
			new DeletionDeltaRuleGen(system, true);
		} catch (IOException i) {
			System.err.println("I/O error");
			i.printStackTrace();
			fail();
		} catch (ParseException p) {
			System.err.println("Parse error");
			p.printStackTrace();
			fail();
		} catch (SAXException e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		} catch (Exception e) {
			System.err.println("XML parse error");
			e.printStackTrace();
			fail();
		}
	}

	public void testCleanDB () throws Exception
	{
		try {
			FlatFileRepositoryDAO dao = new FlatFileRepositoryDAO(TestDeltaRules.SCHEMAFILE);
			OrchestraSystem system = dao.loadAllPeers();
			List<String> tables = BasicEngine.getNamesOfAllTables(dao.loadAllPeers(), true, true, true);
			SqlDb d = new SqlDb(tables, system.getAllSchemas(), system);
			IEngine tcd = new SqlEngine(d, //null, 
					system, true);
		
			tcd.dropAllTables();
		} catch (IOException i) {
			System.err.println("I/O error");
			i.printStackTrace();
			fail();
		} catch (ParseException p) {
			System.err.println("Parse error");
			p.printStackTrace();
			fail();
		}
	}
}
