/**
 * Transforms the abstract datalog rules from datalog-rules.txt into rules adhering
 * to the currently loaded OWL ontology and executes them.
 */
package edu.upenn.cis.orchestra.owldb;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.dbms.SqlDb;

/**
 * @author auer
 *
 */
public class reasoner {
	private Map<String,String> trans=new HashMap<String,String>();
	private Integer maxid;
	private String owlnsid;
	private SqlDb db;
	public void createDatalogProgram(OrchestraSystem catalog) {
		String rules = readFile("src\\main\\java\\edu\\upenn\\cis\\orchestra\\owldb\\datalog-rules.txt");
		// remove comments
		rules = regexReplace("\n#[^\n]*",rules,"");
		rules = regexReplace("\n/[*].*[*]/",rules,"");

		rules = regexReplace("\n\n[0-9]*[a-z]*: ",rules,"@");
		rules = regexReplace("\n\t*",rules,"");
		rules = regexReplace("@",rules,"\n");
		rules = regexReplace("0: ",rules,"");

		// replace shortcuts
		rules = regexReplace("stm",rules,"owldb.OWLDB.STATEMENTS");
		rules = regexReplace("rest",rules,"owldb.OWLDB.RESTRICTIONS");
		rules = regexReplace("list",rules,"owldb.OWLDB.LISTS");
		rules = regexReplace("add",rules,"BUILTINS.ARITH.INTADD");
		rules = regexReplace("sub\\(",rules,"BUILTINS.ARITH.INTSUB(");
		rules = regexReplace("less",rules,"BUILTINS.COMPARE.INTLESS");
		rules = regexReplace("leq",rules,"BUILTINS.COMPARE.INTLESSEQUAL");
		rules = regexReplace("neq",rules,"BUILTINS.COMPARE.INTNOTEQUAL");

		// Additional attributes for statements and lists relations
		rules = regexReplace("(owldb.OWLDB.STATEMENTS)\\(([^\\)]*)\\)",rules,"$1('1',$2,'r')");
		rules = regexReplace("(owldb.OWLDB.LISTS)\\(([^\\)]*)\\)",rules,"$1($2,'r')");

		db = (SqlDb) catalog.getMappingDb();
		Map<String,String> ns = new HashMap<String,String>();
		ns.put("http://www.w3.org/2002/07/owl#","owl");
		ns.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdf");
		ns.put("http://www.w3.org/2000/01/rdf-schema#","rdfs");
		try {
			ResultSet r = db.evaluateQuery("SELECT ns.ns,resources.name,resources.id FROM resources INNER JOIN NS ON(resources.ns=ns.id)"+
					" WHERE NS.ns='http://www.w3.org/2002/07/owl#' OR NS.ns='http://www.w3.org/1999/02/22-rdf-syntax-ns#' OR NS.ns='http://www.w3.org/2000/01/rdf-schema#'");
			while(r.next()) {
				trans.put(ns.get(r.getString("ns"))+":"+r.getString("name"), r.getString("id"));
			}
			owlnsid=getOne("SELECT ns.id FROM NS WHERE NS.ns='http://www.w3.org/2002/07/owl#'");
			r=db.evaluateQuery("SELECT id FROM resources ORDER BY id DESC FETCH FIRST 1 ROWS ONLY");
			r.next();
			maxid=r.getInt(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		createResource("Nothing");
		createResource("Thing");
		createResource("complementOf");
		createResource("equivalentClass");
		createResource("equivalentProperty");
		createResource("TransitiveProperty");
		
		for(String key:trans.keySet()) {
			rules=regexReplace(key,rules,trans.get(key));
		}
		System.out.println(trans.toString());
		System.out.println(rules);
		writeFile("rules.datalog",rules);
	}
	private static String regexReplace(String regex, String str,String replacement) {
		Pattern p = Pattern.compile(regex);
		Matcher m = p.matcher(str);
		return m.replaceAll(replacement);
	}
	private static String readFile(String file) {
		String contents = "";
		try {
			BufferedReader in = new BufferedReader(new FileReader(file));
			while(in.ready()) {
				contents+=in.readLine()+"\n";
			}
			in.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return contents;
	}
	private static void writeFile(String file, String contents) {
		try {
			BufferedWriter out = new BufferedWriter(new FileWriter(file));
			out.write(contents);
			out.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void createResource(String res) {
		if(!trans.containsKey("owl:"+res)) {
			db.evaluate("INSERT INTO resources VALUES ("+(++maxid)+","+owlnsid+",'"+res+"')");
			trans.put("owl:"+res,maxid.toString());
		}
	}
	private String getOne(String query) {
		ResultSet r;
		try {
			r = db.evaluateQuery(query);
			r.next();
			return r.getString(1);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return new String();
	}
}
