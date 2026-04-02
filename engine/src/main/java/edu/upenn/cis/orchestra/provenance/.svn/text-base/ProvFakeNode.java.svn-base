package edu.upenn.cis.orchestra.provenance;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.datamodel.AtomArgument;
import edu.upenn.cis.orchestra.datamodel.OrchestraSystem;
import edu.upenn.cis.orchestra.datamodel.Relation;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;

public class ProvFakeNode extends ProvenanceNode {

	public ProvFakeNode(String semiringName) {
		super(semiringName);
	}

	public ProvFakeNode copySelf() {
		return new ProvFakeNode(_semiringName);
	}

	public String toString() {
		return "DUMMY()";
	}

	public List<Object> getStringExpr() {
		List<Object> results = new ArrayList<Object>();

		results.add("DUMMY()");
		return results;
	}

	public List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException {
		List<Object> results = new ArrayList<Object>();

		results.add(ProvEdbNode.defaultValue(_semiringName));
		return results;
	}

}
