package edu.upenn.cis.orchestra.exchange.tukwila;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import edu.upenn.cis.orchestra.Config;
import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datalog.Datalog;
import edu.upenn.cis.orchestra.datalog.DatalogProgram;
import edu.upenn.cis.orchestra.datalog.DatalogSequence;
import edu.upenn.cis.orchestra.datalog.RecursiveDatalogProgram;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Peer;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.datamodel.RelationContext;
import edu.upenn.cis.orchestra.datamodel.RelationField;
import edu.upenn.cis.orchestra.datamodel.Atom;
import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.AtomConst;
import edu.upenn.cis.orchestra.datamodel.PrimaryKey;
import edu.upenn.cis.orchestra.datamodel.Schema;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Atom.AtomType;
import edu.upenn.cis.orchestra.dbms.IDb;
import edu.upenn.cis.orchestra.dbms.TukwilaDb;
import edu.upenn.cis.orchestra.exchange.sql.CreateProvenanceStorageSql;
import edu.upenn.cis.orchestra.exchange.BasicEngine;
import edu.upenn.cis.orchestra.exchange.CreateProvenanceStorage;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.repository.dao.flatfile.grammar.ParseException;

public class TukwilaEngine extends BasicEngine {

	protected TukwilaDb _db;
	protected DocumentBuilder _builder;
	protected Transformer _trans;
	protected int _nextId = 0;
	protected String _filename = Config.getProperty("tukwilafiles");
	
	public TukwilaEngine(TukwilaDb db, 
			//IDb updateDb, 
			OrchestraSystem system,
			boolean generateRules) throws Exception {
		super(db, //updateDb, 
				system, generateRules);
		_db = db;
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		TransformerFactory tfact = TransformerFactory.newInstance();
        try {
        	_builder = factory.newDocumentBuilder();
        	_trans = tfact.newTransformer();
        	_trans.setOutputProperty(OutputKeys.ENCODING, "iso-8859-1");
        	_trans.setOutputProperty(OutputKeys.INDENT, "yes");
        	_trans.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        	_trans.setOutputProperty(OutputKeys.METHOD, "xml"); //xml, html, text
        	_trans.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
        } catch (ParserConfigurationException pce) {
        	assert(false); 	// can't happen
        } catch (TransformerConfigurationException tce) {
        	assert(false); 	// can't happen
        	
        }
	}

	protected CreateProvenanceStorage createProvenanceStorage() {
		return new CreateProvenanceStorageTukwila(_db);
	}

	public void clearAllTables() throws Exception {
		// TODO Auto-generated method stub

	}

	public void dropAllTables() throws Exception {
		// TODO Auto-generated method stub

	}
	
	public void copyBaseTables() throws Exception {
		// TODO Auto-generated method stub

	}

	public void compareBaseTablesWithCopies() throws Exception {
		// TODO Auto-generated method stub

	}
	
	public void subtractLInsDel() throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	public CreateProvenanceStorageSql getProvenancePrepInfo() {
		// TODO Auto-generated method stub
		return null;
	}

	protected void mapSequence(Document doc, Element parent, DatalogSequence seq) {
		if (seq.isRecursive()) {
			parent = addWhileOp(doc, parent, seq.count4fixpoint());
		}
		parent = addUnionOp(doc, parent, seq.isRecursive() || seq.count4fixpoint());
		for (Datalog d : seq.getSequence()) {
			if (d instanceof DatalogProgram) {
				DatalogProgram prog = (DatalogProgram)d;
				mapProgram(doc, parent, prog);
			} else {
				DatalogSequence subseq = (DatalogSequence)d;
				mapSequence(doc, parent, subseq);
			}
		}
	}

	protected void mapProgram(Document doc, Element parent, DatalogProgram prog) {
		boolean recursive = prog instanceof RecursiveDatalogProgram; 
		if (recursive) {
			parent = addWhileOp(doc, parent, prog.count4fixpoint());
		}
		parent = addUnionOp(doc, parent, recursive || prog.count4fixpoint());
		for (Rule rule : prog.getRules()) {
			mapRule(doc, parent, rule);
		}
	}

	protected String getTypedName(Atom atom) {
		AbstractRelation rel = atom.getRelation();
		AtomType type = atom.getType();
		return rel.getName() + Atom.typeToSuffix(type);
	}

	protected Element addClear(Document doc, Element parent, Atom atom) {
		AbstractRelation rel = atom.getRelation();
		Element write = addBWrite(doc, parent, rel, getTypedName(atom));
		addSchema(doc, write, rel, true);
		return write;
	}

	protected Element mapClearRule(Document doc, Element parent, Rule rule) {
		// !Rxy :- (empty) means clear R
		return addClear(doc, parent, rule.getHead());
	}

	protected Element mapDeleteFromHeadRule(Document doc, Element parent, Rule rule) {
		// Rxy :- Sxyz, !Txy  means  remove all Rxy such that Txy
		Atom head = rule.getHead();
		List<Atom> body = rule.getBody();
		assert(body.size() == 2);
		assert(!body.get(0).isNeg());
		assert(body.get(1).isNeg());
		Element del = addInsDelOp(doc, parent, getTypedName(head), true);
		Element ren1 = addRename(doc, del, head, false);
		Element proj = addProject(doc, ren1, head);
		Atom source = body.get(1);
		Element ren2 = addRename(doc, proj, source, true);
		addBtreeOp(doc, ren2, getTypedName(source), "bscan");
		return del;
	}


	protected Element mapClearNCopyRule(Document doc, Element parent, Rule rule) {
		// Rxy :- Sxy means set R := S and clear S
		List<Atom> body = rule.getBody();
		assert(body.size() == 1);
		Atom head = rule.getHead();
		Atom atom = body.get(0);
		parent = addUnionOp(doc, parent, false);
		Element bwrite = addBWrite(doc, parent, head.getRelation(), getTypedName(head));
		bwrite.setAttribute("output", "false");
		addBtreeOp(doc, bwrite, getTypedName(atom), "bscan");
		addClear(doc, parent, atom);
		return parent;
	}

	protected Element addRename(Document doc, Element parent, Atom atom, boolean toVarNames) {
		Element rename = newRename(doc, atom, toVarNames);
		parent.appendChild(rename);
		return rename;
	}
	
	protected Element newRename(Document doc, Atom atom, boolean toVarNames) {
		Element rename = newQop(doc, "rename");
		List<AtomArgument> lv = atom.getValues();
		AbstractRelation rel = atom.getRelation();
		for (int i = 0; i < lv.size(); i++) {
			String valName = getNameForValue(atom, i);
			String from = toVarNames ? rel.getField(i).getName() : valName;
			String to = toVarNames ? valName : rel.getField(i).getName();
			addChild(doc, rename, "from", from);
			addChild(doc, rename, "to", to);
		}
		return rename;
	}
	
	protected Element addProject(Document doc, Element parent, Atom atom) {
		Element proj = newProject(doc, atom);
		parent.appendChild(proj);
		return proj;
	}

	protected Element newProject(Document doc, Atom atom) {
		List<AtomArgument> lv = atom.getValues();
		Element proj = newQop(doc, "project");
		for (int i = 0; i < lv.size(); i++) {
			addChild(doc, proj, "attribute", getNameForValue(atom, i)); 
		}
		return proj;
	}

//	protected Element addRenameProject(Document doc, Element parent, ScMappingAtom head) {
//		Element rename = addRename(doc, parent, head);
//		return addProject(doc, rename, head);
//	}
	
	protected Element addInsDelOp(Document doc, Element parent, String name, boolean del) {
		Element update = addBtreeOp(doc, parent, name, del ? "bdel" : "bins");
		update.setAttribute("output", del ? "false" : "true");
		return update;
	}

	protected Comparator<Atom> _cmpy = new Comparator<Atom>() {
		public int compare(Atom a1, Atom a2) {
			// Order is as follows: negative atoms right of anything;
			// big atoms left of small atoms; ties broken by strcmp
			boolean n1 = a1.isNeg();
			boolean n2 = a2.isNeg();
			if (n1 && !n2) {
				return 1;
			} else if (!n1 && n2) {
				return -1;
			}
			boolean b1 = (a1.getType() == AtomType.NONE ||
						  a1.getType() == AtomType.INS);
			boolean b2 = (a2.getType() == AtomType.NONE ||
					      a2.getType() == AtomType.INS);
			if (b1 && !b2) {
				return -1;
			} else if (!b1 && b2) {
				return 1;
			}
			return getTypedName(a1).compareTo(getTypedName(a2));
		}
	};

	protected void addNoDups(ArrayList<String> list, String str) {
		if (!list.contains(str)) {
			list.add(str);
		}
	}
	protected ArrayList<String> atomValues(Atom atom) {
		ArrayList<String> list = new ArrayList<String>();
		for (AtomArgument val : atom.getValues()) {
			if (!isConstant(val)) {
				addNoDups(list, val.toString());
			}
		}
		return list;
	}

	protected Element addConstantsSelection(Document doc, Element parent, Atom atom) {
		Element select = addQop(doc, parent, "select");
		List<AtomArgument> cv = new ArrayList<AtomArgument>();
		for (AtomArgument value : atom.getValues()) {
			if (isConstant(value)) {
				cv.add(value);
			}
		}
		assert(cv.size() != 0);
		Element eq = null;
		for (int i = 0; i < cv.size(); i++) {
			String key = valueToField(cv.get(i), atom).getName();
			String value = getNameForValue(atom, cv.get(i));
			if (i == 0) {
				eq = newEqual(doc, key, value);
			} else {
				Element and = newOp(doc, "and");
				and.appendChild(eq);
				and.appendChild(newEqual(doc, key, value));
				eq = and;
			}
		}
		select.appendChild(eq);
		return select;
	}

	protected Element mapNormalRule(Document doc, Element parent, Rule rule) {
		// Rxy :- Sxz, Tzy means just what it says
		Atom head = rule.getHead();
		Element tt = addInsDelOp(doc, parent, getTypedName(head), head.isNeg());
		Element tb = addRename(doc, tt, head, false);
		tb = addProject(doc, tb, head);
		tb = addNeededConstants(doc, tb, head, true);

		// make a sorted copy of the body
		ArrayList<Atom> copy = new ArrayList<Atom>(rule.getBody());
		Collections.sort(copy, _cmpy);
		assert(copy.size() > 0);
		Atom base = copy.get(0);
		assert(!base.isNeg());

		// construct the join tree according to the sort order
		// first set up the base case
		Element bt, bb;
		if (containsConstant(base)) {
			bt = newProject(doc, base); 
			bb = addRename(doc, bt, base, true);
			bb = addConstantsSelection(doc, bb, base);
			bb = addNeededConstants(doc, bb, base, false);
		} else {
			bt = bb = newRename(doc, base, true);
		}
		addBtreeOp(doc, bb, getTypedName(base), "bscan");
		ArrayList<String> vals = atomValues(base);
		// now the iterative case (we add a join or semijoin each time)
		for (int i = 1; i < copy.size(); i++) {
			bt = nextJoin(doc, bt, vals, copy.get(i));
		}
		tb.appendChild(bt);
//		try {_db.execute(elem2xml(tt), Config.FULL_DEBUG); } catch (IOException e) {};
		return tt;
	}

	protected String getNameForValue(Atom atom, int i) {
		AtomArgument v = atom.getValues().get(i);
		if (isConstant(atom.getValues().get(i))) {
			return getNameForConstant(atom, i);
		} else {
			return v.toString();
		}
	}
	
	protected boolean isIndexed(Atom atom, String val) {
		RelationField field = valueToField(val, atom);
		for (RelationField key : atom.getRelation().getPrimaryKey().getFields()) {
			if (key.equals(field)) {
				return true;
			}
		}
		return false;
	}
	
	protected Element nextJoin(Document doc, Element left, List<String> vals, Atom atom) {
		ArrayList<AtomArgument> keys = new ArrayList<AtomArgument>();
		ArrayList<String> nonKeys = new ArrayList<String>();
		for (AtomArgument val : atom.getValues()) {
			String s = val.toString();
			if (isConstant(val) || vals.contains(s)) {
				keys.add(val);
			} else {
				nonKeys.add(s);
			}
		}
		left = addNeededConstants(doc, left, atom, false);
		Element result;
		if (atom.isNeg()) {
			result = addASJoin(doc, left, atom, keys);
		} else {
			result = addJoin(doc, left, atom, keys);
		}
		result = postJoin(doc, result, atom, nonKeys, vals);
		vals.addAll(nonKeys);
		return result;
	}

	protected Element addNeededConstants(Document doc, Element node, Atom atom, boolean under) {
		List<AtomArgument> lv = atom.getValues();
		for (int i = 0; i < lv.size(); i++) {
			AtomArgument v = lv.get(i);
			if (isConstant(v)) {
				String name = getNameForConstant(atom, i);
				AbstractRelation rel = atom.getRelation();
				RelationField f = rel.getFields().get(i);
				String type = f.getType().getXSDType();
				if (isNull(v)) {
					node = addNull(doc, node, name, type, under);
				} else {
					node = addConstant(doc, node, name, type, v.toString(), under);
				}
			}
		}
		return node;
	}

	protected boolean containsConstant(ArrayList<Atom> atoms) {
		for (Atom atom : atoms) {
			if (containsConstant(atom)) {
				return true;
			}
		}
		return false;
	}

	protected boolean containsConstant(Atom atom) {
		for (AtomArgument v : atom.getValues()) {
			if (isConstant(v)) {
				return true;
			}
		}
		return false;
	}

	protected boolean isConstant(AtomArgument v) {
		assert(v instanceof AtomConst || !v.toString().equals("-"));
		return (v instanceof AtomConst);
	}

	protected boolean isNull(AtomArgument v) {
		if (v instanceof AtomConst) {
			AtomConst c = (AtomConst)v;
			if (c.getValue() == null) {
				return true;
			}
		} else {
			assert(!v.toString().equals("-"));
		}
		return false;
	}
	
	protected boolean isNull(String s) {
		return s.equals("-");
	}
	
	protected Element addNull(Document doc, Element node, String name, String type, boolean under) {
		Element el = newNull(doc, name, type);
		mount(node, el, under);
		return el;
	}

	protected String getNameForValue(Atom atom, AtomArgument value) {
		List<AtomArgument> lv = atom.getValues();
		for (int i = 0; i < lv.size(); i++) {
			if (lv.get(i).equals(value)) {
				return getNameForValue(atom, i);
			}
		}
		return null;
	}

	protected String getNameForConstant(Atom atom, int i) {
		assert(isConstant(atom.getValues().get(i)));
		AtomConst c = (AtomConst)atom.getValues().get(i);
		RelationField f = atom.getRelation().getField(i);
		return f.getName() + "_" + c.getValue();
	}

	protected Element newNull(Document doc, String name, String type) {
		Element text = newQop(doc, "text");
		addChild(doc, text, "label", name);
		addChild(doc, text, "type", type);
		Element value = addChild(doc, text, "value", type);
		value.setAttribute("type", type);
		value.setAttribute("isNull", "true");
		value.setTextContent(".");	// must be non-empty but otherwise ignored
		return text;
	}

	protected Element subConstant(Document doc, Element child, String name, String type, String value) {
		Element el = newConstant(doc, name, type, value);
		el.appendChild(child);
		return el;
	}

	protected Node mount(Element left, Element right, boolean under) {
		if (under) {
			return left.appendChild(right);
		} else {
			return right.appendChild(left);
		}
	}
	
	protected Element addConstant(Document doc, Element node, String name, String type, String value, boolean under) {
		Element el = newConstant(doc, name, type, value);
		mount(node, el, under);
		return el;
	}

	protected Element newConstant(Document doc, String name, String type, String value) {
		Element text = newQop(doc, "text");
		addChild(doc, text, "label", name);
		addChild(doc, text, "type", type);
		Element val = addChild(doc, text, "type", type);
		val.setAttribute("type", type);
		val.setTextContent("value");
		return text;
	}

	protected Element addEqual(Document doc, Element parent, String key, String value) {
		Element eq = newEqual(doc, key, value);
		parent.appendChild(eq);
		return eq;
	}

	protected Element newEqual(Document doc, String key, String value) {
		Element eq = newOp(doc, "equal");
		Element op1 = addOp(doc, eq, "variable");
		addChild(doc, op1, "name", key);
		Element op2 = addOp(doc, eq, "variable");
		addChild(doc, op2, "name", value);
		return eq;
	}

	protected Element postJoin(Document doc, Element join, Atom atom, List<String> nonkeys, List<String> vals) {
		// attributes of join result is a cross-product of schemas; 
		// e.g. Rxy, Syz where R(A,B) and S(C,D), the join results in 
		// attributes "x,y,C,D".  need to rename D -> z and project 
		// away C to obtain "x,y,z"
		if (atom.isNeg() && !containsConstant(atom)) {
			// no projection needed in this case
			return join;
		}
		Element top, proj;
		if (atom.isNeg()) { // i.e. join was anti-semijoin
			// rename not relevant in this case, but project still needed
			// if extra columns were added
			top = proj = newQop(doc, "project");
		} else {
			top = newQop(doc, "rename");
			for (String s : nonkeys) {
				RelationField f = valueToField(s, atom);
				addChild(doc, top, "from", f.getName());
				addChild(doc, top, "to", s);
			}
			proj = addQop(doc, top, "project");
		}
		for (String s : vals) {
			addChild(doc, proj, "attribute", s);
		}
		for (String s : nonkeys) {
			if (!isNull(s)) {
				RelationField f = valueToField(s, atom);
				addChild(doc, proj, "attribute", f.getName());
			}
		}
		proj.appendChild(join);
		return top;
	}

/*
  <qoperator id="Node10" joinType="conventional" operation="join">
<!--    <joinPredicate>
      <value type="xsd:boolean">true</value>
    </joinPredicate>
-->
    <joinConstraint>
      <attribute>$C_CK</attribute>
      <attribute>$O_CK</attribute>
    </joinConstraint>
    <signature length="2">2</signature>
    <signature length="2">1</signature>
    <iterator alternate="true" type="adaptive">
      <signature length="2">2</signature>
      <signature length="2">1</signature>
    </iterator>
    <structure buckets="430" cardinality="0" name="Node10L" size="1762304" type="hash">
      <schema/>
    </structure>
    <structure name="Node10R" type="btree" filename="foo.bdb" dbname="dude" overwrite="false">
      <schema/>
    </structure>
    <qoperator ... />
  </>
*/
	protected void addSignature(Document doc, Element parent) {
		Element sig = addChild(doc, parent, "signature", "2");
		sig.setAttribute("length", "2");
		sig = addChild(doc, parent, "signature", "1");
		sig.setAttribute("length", "2");
	}
	
	protected Element addJoin(Document doc, Element left, Atom right, List<AtomArgument> keys) {
		Element join = newQop(doc, "join");
		join.setAttribute("joinType", "conventional");
		//Element pred = addChild(doc, join, "joinPredicate");
		Element value = addChild(doc, join, "value", "true");
		value.setAttribute("type", "xsd:boolean");
		for (AtomArgument key : keys) {
			Element constraint = addChild(doc, join, "joinConstraint");
			addChild(doc, constraint, "attribute", getNameForValue(right, key));
			RelationField f = valueToField(key, right);
			addChild(doc, constraint, "attribute", f.getName());
		}
		addSignature(doc, join);
		Element it = addChild(doc, join, "iterator");
		it.setAttribute("alternate", "true");
		it.setAttribute("type", "nested");
		addSignature(doc, it);
		Element lstruct = addChild(doc, join, "structure");
		lstruct.setAttribute("name", join.getAttribute("id") + "L");
		lstruct.setAttribute("buckets", "4");
		lstruct.setAttribute("cardinality", "0");
		lstruct.setAttribute("size", "4096");
		lstruct.setAttribute("type", "hash");
		addChild(doc, lstruct, "schema");
		Element rstruct = addChild(doc, join, "structure");
		rstruct.setAttribute("name", join.getAttribute("id") + "R");
		rstruct.setAttribute("type", "btree");
		rstruct.setAttribute("filename", _filename);
		rstruct.setAttribute("dbname", getTypedName(right));
		addChild(doc, rstruct, "schema");
		join.appendChild(left);
		addSchema(doc, join, right.getRelation(), true);
		return join;
	}

/*
  <qoperator id="Node10" operation="accsjoin" semijoin="false">
    <structure name="Node10R" type="btree" filename="foo.bdb" dbname="nation" overwrite="false">
      <schema/>
    </structure>
    <sourceKeys>
      <attribute>$N_REGIONKEY</attribute>
    </sourceKeys>
    <matchKeys>
      <attribute>$N_REGIONKEY</attribute>
    </matchKeys>
    <matchSchema>
      <attribute name="$N_NAME" type="xsd:string"/>
      <attribute name="$N_NATIONKEY" type="xsd:int"/>
      <attribute name="$N_REGIONKEY" type="xsd:int"/>
    </matchSchema>
    <qoperator filename="foo.bdb" dbname="nation" id="Node0" operation="bscan"/>
  </qoperator>
 */
	protected Element addASJoin(Document doc, Element left, Atom right, List<AtomArgument> keys) {
		Element semijoin = newQop(doc, "accsjoin");
		semijoin.setAttribute("semijoin", "false");
		addStructureOp(doc, semijoin, right);
		
		Element sourceKeys = addChild(doc, semijoin, "sourceKeys");
		Element matchKeys = addChild(doc, semijoin, "matchKeys");
		for (AtomArgument key : keys) {
			addChild(doc, sourceKeys, "attribute", getNameForValue(right, key));
			RelationField f = valueToField(key, right);
			addChild(doc, matchKeys, "attribute", f.getName());
		}
		Element schema = addChild(doc, semijoin, "matchSchema");
		for (RelationField f : right.getRelation().getFields()) {
			Element a = addChild(doc, schema, "attribute");
			a.setAttribute("name", f.getName());
			a.setAttribute("type", f.getType().getXSDType());
		}
		semijoin.appendChild(left);
		return semijoin;
	}

	protected RelationField valueToField(AtomArgument value, Atom atom) {
		List<AtomArgument> lv = atom.getValues();
		List<RelationField> lf = atom.getRelation().getFields();
		assert(lv.size() == lf.size());
		for (int i = 0; i < lv.size(); i++) {
			if (lv.get(i).equals(value)) {
				return lf.get(i);
			}
		}
		return null;
	}

	protected RelationField valueToField(String value, Atom atom) {
		List<AtomArgument> lv = atom.getValues();
		List<RelationField> lf = atom.getRelation().getFields();
		assert(lv.size() == lf.size());
		for (int i = 0; i < lv.size(); i++) {
			if (lv.get(i).toString().equals(value)) {
				return lf.get(i);
			}
		}
		return null;
	}

	protected String fieldToValue(RelationField field, Atom atom) {
		List<AtomArgument> lv = atom.getValues();
		List<RelationField> lf = atom.getRelation().getFields();
		assert(lv.size() == lf.size());
		for (int i = 0; i < lf.size(); i++) {
			if (lf.get(i).equals(field)) {
				return lv.get(i).toString();
			}
		}
		return null;
	}

	protected void mapRule(Document doc, Element parent, Rule rule) {
		Atom head = rule.getHead();
		List<Atom> body = rule.getBody();
		
		Element result;
		if (rule.toString().startsWith("INS_P.S.P0_S0_R0(KID__,RN__,-,-)")) {
			// breakpoint here
			rule.toString();
		}
		if (body.size() == 0) {
			assert(head.isNeg());
			result = mapClearRule(doc, parent, rule);
			if (Config.getFullDebug()) { addChild(doc, result, "DEBUG_CASE_1", rule.toString()); }
		} else if (rule.getDeleteFromHead()) {
			result = mapDeleteFromHeadRule(doc, parent, rule);
			if (Config.getFullDebug()) { addChild(doc, result, "DEBUG_CASE_2", rule.toString()); }
		} else if (rule.clearNcopy()) {
			result = mapClearNCopyRule(doc, parent, rule);
			if (Config.getFullDebug()) { addChild(doc, result, "DEBUG_CASE_3", rule.toString()); }
		} else {
			result = mapNormalRule(doc, parent, rule);
			if (Config.getFullDebug()) { addChild(doc, result, "DEBUG_CASE_4", rule.toString()); }
		}
	}
	
	/*
	 <qoperator operation="while" output="true">
	   <operator operation="notequal">
         <operator operation="variable">
           <name>$COUNT</name>
         </operator>
         <value type="xsd:int">0</value>
       </operator>
     </qoperator>
     */
	protected Element addWhileOp(Document doc, Element parent, boolean output) {
		Element whileOp = addQop(doc, parent, "while");
		whileOp.setAttribute("output", output ? "true" : "false");
		Element ne = addOp(doc, whileOp, "notequal");
		Element var = addOp(doc, ne, "variable");
		addChild(doc, var, "name", "$COUNT");
		Element val = addChild(doc, ne, "value", "0");
		val.setAttribute("type", "xsd:int");
		return whileOp;
	}

	protected Element addUnionOp(Document doc, Element parent, boolean output) {
		Element union = newUnionOp(doc, output);
		parent.appendChild(union);
		return union;
	}

	protected Element newUnionOp(Document doc, boolean output) {
		Element union = newQop(doc, "union");
		union.setAttribute("output", output ? "true" : "false");
		addChild(doc, union, "combineType", "switch");
		return union;
	}
	
//	/*
//<qoperator id="Node1" operation="tupleout">
//  <qoperator operation="text" id="Node2">
//    <label>$COUNT_2</label>
//    <type>xsd:int</type>
//    <value type="xsd:int">
//      0
//    </value>
//    <qoperator filename="foo.bdb" dbname="nation" id="Node0" operation="bscan"/>
//  </qoperator>
//</qoperator>
//	 */
//	protected Element addZeroCountOp(Document doc, Element parent) {
//		String type = "xsd:int";
//		String label = "temp";
//		Element ren = addQop(doc, parent, "rename");
//		addChild(doc, ren, "from", label);
//		addChild(doc, ren, "to", "$COUNT");
//		Element proj = addQop(doc, ren, "project");
//		addChild(doc, proj, "attribute", label);
//		Element text = addQop(doc, proj, "text");
//		addChild(doc, text, "label", label);
//		addChild(doc, text, "type", type);
//		Element value = addChild(doc, text, "value");
//		value.setAttribute("type", type);
//		value.setTextContent("0");
//		return text;
//	}
//
	protected void mapSequenceList(List<DatalogSequence> rules, int count) throws IOException {
		assert(rules.size() == 3);	// ... but doesn't really matter
//		for (int i = 0; i < rules.size(); i++) {
		for (int i = 0; i < count; i++) {
			DatalogSequence seq = rules.get(i);
			resetNextId();
			Document doc = _builder.newDocument();
			Element root = rootQop(doc, "tupleout");	// TODO: remove for timings
			mapSequence(doc, root, seq);
			assert(root.getChildNodes().getLength() == 1);
			doc.removeChild(root);
			doc.appendChild(root.getFirstChild());
			_db.execute(doc2xml(doc), Config.getApply());
		}
	}
	
	public long mapUpdates(int lastrec, int recno, Peer p, boolean insFirst) throws IOException {
		if (Config.getInsert()) {
			Debug.println("MAPPING INSERTION UPDATES:");
			List<DatalogSequence> rules = getIncrementalInsertionProgram();
			mapSequenceList(rules, rules.size());
			//mapSequenceList(rules, 2);
		}
		if (Config.getDelete()) {
			Debug.println("MAPPING DELETION UPDATES:");
			mapSequenceList(getIncrementalDeletionProgram(), 2);	
		}
		return 0;
	}
	
	static public String toString(Document document) {
        String result = null;
        
        if (document != null) {
            StringWriter strWtr = new StringWriter();
            StreamResult strResult = new StreamResult(strWtr);
            TransformerFactory tfac = TransformerFactory.newInstance();
            try {
                Transformer t = tfac.newTransformer();
                t.setOutputProperty(OutputKeys.ENCODING, "iso-8859-1");
                t.setOutputProperty(OutputKeys.INDENT, "yes");
                t.setOutputProperty(OutputKeys.METHOD, "xml"); //xml, html, text
                t.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
                t.transform(new DOMSource(document.getDocumentElement()), strResult);
            } catch (Exception e) {
                System.err.println("XML.toString(Document): " + e);
            }
            result = strResult.getWriter().toString();
        }

        return result;
    }

	protected String doc2xml(Document doc) {
        StringWriter writer = new StringWriter();
        StreamResult result = new StreamResult(writer);
		try {
			_trans.transform(new DOMSource(doc.getDocumentElement()), result);
			return result.getWriter().toString();
		} catch (TransformerException te) {
			assert(false); // what happened?
			return null;
		}
	}

	protected String elem2xml(Element elem) {
		Document doc = _builder.newDocument();
		Element copy = (Element)doc.importNode(elem, true);
		doc.appendChild(copy);
		String str = doc2xml(doc);
		return str;
	}

	protected Element addChild(Document doc, Element parent, String name) {
		Element child = doc.createElement(name);
		parent.appendChild(child);
		return child;
	}

	protected Element addChild(Document doc, Element parent, String name, String text) {
		Element child = doc.createElement(name);
		parent.appendChild(child);
		child.setTextContent(text);
		return child;
	}

	protected void resetNextId() {
		_nextId = 0;
	}
	
	protected Element rootQop(Document doc, String operation) {
		Element child = doc.createElement("qoperator");
		child.setAttribute("id", "Node" + _nextId++);
		child.setAttribute("operation", operation);
		doc.appendChild(child);
		return child;
	}

	protected Element newOp(Document doc, String operation) {
		Element child = doc.createElement("operator");
		child.setAttribute("operation", operation);
		return child;
	}

	protected Element addOp(Document doc, Element parent, String operation) {
		Element child = newOp(doc, operation);
		parent.appendChild(child);
		return child;
	}

	protected Element addQop(Document doc, Element parent, String operation) {
		Element child = newQop(doc, operation);
		parent.appendChild(child);
		return child;
	}
	
	protected Element newQop(Document doc, String operation) {
		Element child = doc.createElement("qoperator");
		if (operation.equals("bwrite") && _nextId == 368) {
			if (Config.getFullDebug() && operation.equals("projectasf")) {
				// add some stack information
				StackTraceElement[] stack = Thread.getAllStackTraces().get(Thread.currentThread());
				for (int i = 2; i < 9; i++) {
					String str = stack[i].getMethodName() + ":" + stack[i].getLineNumber();
					Element el = doc.createElement("DEBUG_STACK_" + (i-1));
					el.setTextContent(str);
					child.appendChild(el);
				}
			}
		}
		child.setAttribute("id", "Node" + _nextId++);
		child.setAttribute("operation", operation);
		return child;
	}

	protected Element addStructureOp(Document doc, Element parent, Atom atom) {
		Element struct = doc.createElement("structure");
		struct.setAttribute("id", "Node" + _nextId++);
		struct.setAttribute("type", "btree");
		struct.setAttribute("filename", _filename);
		struct.setAttribute("dbname", getTypedName(atom));
		struct.setAttribute("overwrite", "false");
		parent.appendChild(struct);
		return struct;
	}

	protected Element addSql(Document doc, Element parent, Relation rel, String suffix) {
		Element sql = addQop(doc, parent, "sql");
		sql.setAttribute("type", "DB2");
		addSchema(doc, sql, rel, false);
		addChild(doc, sql, "host", Config.getProperty("db2host"));
		addChild(doc, sql, "user", Config.getProperty("db2user"));
		addChild(doc, sql, "password", Config.getProperty("db2pwd"));
		addChild(doc, sql, "database", Config.getProperty("db2dbname"));
		addChild(doc, sql, "table", rel.getDbSchema() + "." + rel.getName() + suffix);
		return sql;
	}

	protected Element addSchema(Document doc, Element parent, AbstractRelation rel, boolean qop) {
		Element schema = qop ? addQop(doc, parent, "schema") : addChild(doc, parent, "schema"); 
		for (RelationField f : rel.getFields()) {
			Element a = addChild(doc, schema, "attribute");
			a.setAttribute("name", f.getName());
			a.setAttribute("type", f.getType().getXSDType());
		}
		return schema;
	}
	
	protected Element addBtreeOp(Document doc, Element parent, String dbname, String op) {
		Element update = addQop(doc, parent, op);
		update.setAttribute("filename", _filename);
		update.setAttribute("dbname", dbname);
		return update;
	}
	
	protected Element addBWrite(Document doc, Element parent, AbstractRelation rel, String dbname) {
		Element write = addBtreeOp(doc, parent, dbname, "bwrite");
		write.setAttribute("overwrite", "true");
		Element keys = addChild(doc, write, "keys");
		PrimaryKey k = rel.getPrimaryKey();
		if (k != null) {
			for (RelationField f : k.getFields()) {
				addChild(doc, keys, "attribute", f.getName());
			}
		}
		return write;
	}
	
	/* Sample copy relation plan:
	<qoperator operation="tupleout">
		<qoperator id="Node0" operation="sql" type="DB2">
			<schema>
				<attribute name="KID" type="xsd:int" /> 
				<attribute name="RN" type="xsd:string" /> 
			</schema>
			<host>dbserv.cis.upenn.edu</host> 
			<user>tjgreen</user>
			<password>todd9807</password>
			<database>DEFEAT</database> 
			<table>tjgreen.P0_S0_R0</table>
		</qoperator>
	</qoperator>
		 */
	protected Element copyRelationPlan(Document doc, Element parent, Relation rel, String suffix) {
		Element bwrite = addBWrite(doc, parent, rel, rel.getName() + suffix);
		addSql(doc, bwrite, rel, suffix);
		return bwrite;
	}

	/* Sample new relation plan:
	  <qoperator id="Node2" operation="tupleout">
	    <qoperator id="Node0" operation="bwrite" filename="orchestra.bdb" dbname="nation" overwrite="true">
	      <keys>
	        <attribute>$N_REGIONKEY</attribute>
	      </keys>
	      <qoperator id="Node1" operation="schema">
	        <attribute name="$N_NAME" type="xsd:string"/>
	        <attribute name="$N_NATIONKEY" type="xsd:int"/>
	        <attribute name="$N_REGIONKEY" type="xsd:int"/>
	      </qoperator>
	    </qoperator>
	  </qoperator>
		 */

	protected Element addRelationPlan(Document doc, Element parent, AbstractRelation rel, String suffix) {
		Element write = addBWrite(doc, parent, rel, rel.getName() + suffix);
		addSchema(doc, write, rel, true);
		return write;
	}

	public void importUpdates(IDb sourceDb) throws IOException {
		// Copy the input DB2 relations to Tukwila b-trees
		Debug.println("IMPORT UPDATES");
		resetNextId();
		Document doc = _builder.newDocument();
		Element tupleout = newQop(doc, "tupleout");		
		doc.appendChild(tupleout);
		Element union = addUnionOp(doc, tupleout, true);
		for (Peer p : _system.getPeers()) {
			for (Schema s : p.getSchemas()) {
				for (Relation r : s.getRelations()) {
					String rname = r.getName();
					if (rname.endsWith(Relation.LOCAL)) {
						copyRelationPlan(doc, union, r, Atom.typeToSuffix(AtomType.INS));
					}
					if (rname.endsWith(Relation.REJECT)) {
						copyRelationPlan(doc, union, r, Atom.typeToSuffix(AtomType.DEL));
					}
				}
			}
		}
		String plan = doc2xml(doc);
		_db.execute(plan, Config.getBoolean("import"));
	}

	public void migrate() throws IOException {
		// Create the b-trees for the mapping relations
		Debug.println("MIGRATE UPDATES");
		resetNextId();
		Document doc = _builder.newDocument();
		Element tupleout = newQop(doc, "tupleout");		
		doc.appendChild(tupleout);
		Element union = addUnionOp(doc, tupleout, true);
		for (RelationContext c : _system.getMappingEngine().getMappingRelations()) {
			for (AtomType t : AtomType.values()) {
				addRelationPlan(doc, union, c.getRelation(), Atom.typeToSuffix(t));
			}
		}
		for (Peer p : _system.getPeers()) {
			for (Schema s : p.getSchemas()) {
				for (AbstractRelation r : s.getRelations()) {
					for (AtomType t : AtomType.values()) {
						addRelationPlan(doc, union, r, Atom.typeToSuffix(t));
					}
				}
			}
		}
		String plan = doc2xml(doc);
		_db.execute(plan, Config.getMigrate());
	}

	
	// TODO: create any necessary _L, _R tables, etc.
	public void createBaseSchemaRelations() throws ParseException
	{
		
	}
}
