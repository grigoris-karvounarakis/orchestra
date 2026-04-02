/*
 * OWLDBStatementHandler.java
 */

package edu.upenn.cis.orchestra.owldb;

import java.io.FileWriter;
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.openrdf.model.BNode;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;

/**
 * This class represents an implementation of a StatementHandler, which
 * is used by rio rdf parser.
 *
 * @author Soeren Auer
 * @version 0.1, 2007-10-25
 */
public class OWLDBStatementHandler implements CSVStatementHandler {

	private String nullstr="";
	/*
	 * maps that are used to store the id's of certain namesapces, resources
	 * and literals
	 */
	private Map<String, Integer> namespaceMap;
	private Map<Resource, Integer> resourceMap;
	private Map<Literal, Integer> literalMap;

	/*
	 * some integer objects that are used in order to generate id's for new
	 * namespaces, resources and literals; every time an id is created the
	 * particular counter will be incremented
	 */
	private Integer namespaceCounter;
	private Integer resourceCounter;
	private Integer literalCounter;

	/*
	 * we do not write to the files each time the handleStatement method runs,
	 * for this would end in a very bad performance... so we write the data to
	 * a string
	 */
	private StringBuilder namespaces;
	private StringBuilder resources;
	private StringBuilder literals;
	private StringBuilder statements;

	/*
	 * for each file we want to create we need a FileWriter object
	 */
	private FileWriter namespaceWriter;
	private FileWriter resourceWriter;
	private FileWriter literalWriter;
	private FileWriter statementWriter;

	/**
	 * The constructor of a OWLDBStatementHandler object.
	 */
	public OWLDBStatementHandler() {
		super();

		this.namespaceMap = new HashMap<String, Integer>(100);
		this.resourceMap = new HashMap<Resource, Integer>(100);
		this.literalMap = new HashMap<Literal, Integer>(100);

		this.namespaceCounter = new Integer(0);
		this.resourceCounter = new Integer(0);
		this.literalCounter = new Integer(0);

		// make sure that the needed files exist and that they are empty;
		// this happens only when the object is created, but not each time the
		// handleStatement method runs, for we want to append the new ones to
		// the end of the file
		try {
			this.namespaceWriter = new FileWriter("owldb_namespaces.csv", false);
			this.resourceWriter = new FileWriter("owldb_resources.csv", false);
			this.literalWriter = new FileWriter("owldb_literals.csv", false);
			this.statementWriter = new FileWriter("owldb_statements.csv", false);
		} catch (IOException e) {
			e.printStackTrace();
		}

		this.namespaces = new StringBuilder();
		this.resources = new StringBuilder();
		this.literals = new StringBuilder();
		this.statements = new StringBuilder();

		this.namespaceMap.put("", ++this.namespaceCounter);
		this.namespaces.append(this.namespaceCounter + ",\n");
	}

	/**
	 * @see CSVStatementHandler
	 */
	public void handleStatement(Resource subject, URI predicate, Value object) {
try {
		// check the subject whether it is a URI or a BNode; then check whether
		// the URI (namespace + local name) is in one of the maps or not
		if (subject instanceof URI) {
			URI subURI = (URI)subject;
			// check whether the namespace of this URI is in the namespaceMap
			if (!this.namespaceMap.containsKey(subURI.getNamespace())) {
				this.namespaceMap.put(subURI.getNamespace(),
						++this.namespaceCounter);
				this.namespaceWriter.append(this.namespaceCounter + "," +
						subURI.getNamespace() + "\n");
			}
			// check whether the local name of this URI is in the resourcesMap
			if (!this.resourceMap.containsKey(subURI)) {
				this.resourceMap.put(subURI, ++this.resourceCounter);
				this.resourceWriter.append(this.resourceCounter + "," +
						this.namespaceMap.get(subURI.getNamespace()) + "," +
						subURI.getLocalName().replace(",", "\\,") + "\n");
			}
		} else if (subject instanceof BNode) {
			BNode subBNode = (BNode)subject;
			// check whether the id of this BNode is in the resourcesMap
			if (!this.resourceMap.containsKey(subBNode)) {
				this.resourceMap.put(subBNode, ++this.resourceCounter);
				this.resourceWriter.append(this.resourceCounter + ",0," +
						subBNode.getID() + "\n");
			}
		}

		// process the predicate which is a URI (namespace + local name)
		if (!this.namespaceMap.containsKey(predicate.getNamespace())) {
			this.namespaceMap.put(predicate.getNamespace(),
					++this.namespaceCounter);
			this.namespaceWriter.append(this.namespaceCounter + "," +
					predicate.getNamespace() + "\n");
		}
		if (!this.resourceMap.containsKey(predicate)) {
			this.resourceMap.put(predicate, ++this.resourceCounter);
			this.resourceWriter.append(resourceCounter + "," +
					this.namespaceMap.get(predicate.getNamespace()) + "," +
					predicate.getLocalName().replace(",", "\\,") + "\n");
		}

		// process the object which can be a URI, BNode or Literal
		if (object instanceof URI) {
			URI objURI = (URI)object;
			// check whether the namespace of this URI is in the namespaceMap
			if (!this.namespaceMap.containsKey(objURI.getNamespace())) {
				this.namespaceMap.put(objURI.getNamespace(),
						++this.namespaceCounter);
				this.namespaceWriter.append(this.namespaceCounter + "," +
						objURI.getNamespace() + "\n");
			}
			// check whether the local name of this URI is in the resourcesMap
			if (!this.resourceMap.containsKey(objURI)) {
				this.resourceMap.put(objURI, ++this.resourceCounter);
				this.resourceWriter.append(this.resourceCounter + "," +
						this.namespaceMap.get(objURI.getNamespace()) + "," +
						objURI.getLocalName().replace(",", "\\,") + "\n");
			}
		} else if (object instanceof BNode) {
			BNode objBNode = (BNode)object;
			// check whether the id of this BNode is in the resourcesMap
			if (!this.resourceMap.containsKey(objBNode)) {
				this.resourceMap.put(objBNode, ++this.resourceCounter);
				this.resourceWriter.append(this.resourceCounter + ",1," +
						objBNode.getID() + "\n");
			}
		} else if (object instanceof Literal) {
			Literal objLiteral = (Literal)object;
			// check whether this literal (value + datatype + language) already
			// is in the literalMap
			if (!this.literalMap.containsKey(objLiteral)) {
				this.literalMap.put(objLiteral, ++this.literalCounter);
				this.literalWriter.append(this.literalCounter + ",\"" +
						objLiteral.getLabel().replace("\n", "\\n").replace("\"", "'")
						+ "\",");

				URI datatype = objLiteral.getDatatype();
				if (datatype == null) this.literalWriter.append(this.nullstr+",");
				else if (!this.resourceMap.containsKey(datatype)) {
					this.resourceMap.put(datatype, this.resourceCounter);
					if (!this.namespaceMap.containsKey(datatype.getNamespace())) {
						this.namespaceMap.put(datatype.getNamespace(),++this.namespaceCounter);
						this.namespaceWriter.append(this.namespaceCounter + "," + datatype.getNamespace() + "\n");
					}
					this.resourceWriter.append(++this.resourceCounter + "," + this.namespaceMap.get(datatype.getNamespace()) + "," + datatype.getLocalName() + "\n");
					this.literalWriter.append(this.resourceCounter + ",");
				}
				String langString = (objLiteral.getLanguage());
				if (langString == null) this.literalWriter.append(this.nullstr+"\n");
				else {
					Literal language = new LiteralImpl(langString);
					if (!this.literalMap.containsKey(language)) {
					}
					this.literalMap.put(language, ++this.literalCounter);
					this.literalWriter.append(this.literalCounter + "\n" +
							this.literalCounter + "," + language.getLabel() +
							","+this.nullstr+","+this.nullstr+"\n");
				}
			}
		}

		this.statementWriter.append("1,"+this.resourceMap.get(subject) + "," +
				this.resourceMap.get(predicate) + ",");
		if (object instanceof Resource) {
			this.statementWriter.append(this.resourceMap.get(object) + ",r\n");
		} else {
			this.statementWriter.append(this.literalMap.get(object) + ",l\n");
		}
} catch (IOException e) {
	e.printStackTrace();
}
	}

	/**
	 * @see CSVStatementHandler
	 */
	public void writeDataToFiles() {

		// initialize the file writer... make sure that the files are opened
		// with append-option activated
		try {
			/*this.namespaceWriter = new FileWriter("owldb_namespaces.csv", true);
			this.resourceWriter = new FileWriter("owldb_resources.csv", true);
			this.literalWriter = new FileWriter("owldb_literals.csv", true);
			this.statementWriter = new FileWriter("owldb_statements.csv", true);*/

			//this.namespaceWriter.append(this.namespaces);
			this.namespaceWriter.close();
			//this.resourceWriter.append(this.resources);
			this.resourceWriter.close();
			//this.literalWriter.append(this.literals);
			this.literalWriter.close();
			//this.statementWriter.append(this.statements);
			this.statementWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
