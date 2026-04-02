/**
 * Loader for loading OWL files in RDF/XML format into an Orchestra peer.
 */
package edu.upenn.cis.orchestra.owldb;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.openrdf.rio.ParseErrorListener;
import org.openrdf.rio.Parser;
import org.openrdf.rio.rdfxml.RdfXmlParser;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.dbms.SqlDb;

/**
 * @author auer
 *
 */
public class loader {
	/**
	 * Loads an RDF/XML file into Orchestra's OWLDB schema
	 * 
	 * @param file		The RDF/XML file to convert.
	 * @param BASEURI	BaseURI of the RDF/XML file.
	 * @param catalog
	 * @return
	 * @throws IOException
	 */
	public static boolean loadOWL(String file, String BASEURI, OrchestraSystem catalog) throws IOException {
		createCSV(file, BASEURI);
		SqlDb db = (SqlDb) catalog.getMappingDb();
		File path=new File("src\\main\\java\\edu\\upenn\\cis\\orchestra\\owldb\\");
		db.evaluateFromShell("db_structure.db2", path, false);
		///db.evaluateFromShell("db_procs.db2", path, false);
		Runtime.getRuntime().exec("db2cmd /c /w /i db2 -td@ -f db_procs.db2", null, path);
		db.evaluate("INSERT INTO models (modelID,modelURI,baseURI) VALUES (1, \'"+file+"\', \'"+BASEURI+"\')");
		db.evaluateFromShell("src\\main\\java\\edu\\upenn\\cis\\orchestra\\owldb\\db_load.db2", new File("C:\\Documents and Settings\\auer\\My Documents\\orchestra\\engine"), false);
		//db.evaluate("LOAD FROM \"literals.csv\" OF DEL INSERT INTO literals");

		return true;
	}
	/**
	 * Creates CSV files from RDF/XML for rapid loading into the DB
	 * 
	 * @param filename	The RDF/XML file to convert.
	 * @param BASEURI	BaseURI of the RDF/XML file.
	 */
	public static void createCSV(String filename, String BASEURI) {
		File d=new File(filename);
		String[] files = new String[d.isDirectory()==true?d.list().length:1];
		if (d.isDirectory()==true) {
			String[] f = d.list();
			for(int i=0;i<f.length;i++) if(f[i]!="..") {
				files[i] = filename+"\\"+f[i];
			}
		} else
			files[0] = filename;
	
		System.setProperty("org.xml.sax.driver","org.apache.xerces.parsers.SAXParser");
	
		Parser parser = new RdfXmlParser();
		//Parser parser = new NTriplesParser();
		ParseErrorListener errorListener = new OWLDBParseErrorListener();
		CSVStatementHandler statementHandler = null;
	
		statementHandler = new OWLDBStatementHandler();
	
		parser.setStatementHandler(statementHandler);
		parser.setParseErrorListener(errorListener);
		parser.setVerifyData(true);
		parser.setStopAtFirstError(true);
	
		long timeBefore = 0;
		long timeAfter = 0;
	
		try {
			timeBefore = System.currentTimeMillis();
			for (int i=0; i<files.length; ++i) {
				System.out.print("PARSING: " + files[i] + "...\n");
				parser.parse(new FileInputStream(files[i]), BASEURI);
			}
			timeAfter = System.currentTimeMillis();
			System.out.println("DONE (" + ((timeAfter - timeBefore)/1000.0) + "s)");
			statementHandler.writeDataToFiles();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
