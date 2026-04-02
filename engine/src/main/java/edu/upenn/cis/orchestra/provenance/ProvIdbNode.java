package edu.upenn.cis.orchestra.provenance;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;

public class ProvIdbNode extends ProvenanceNode {
	String _label;
	Integer _labelValue = 1;
	boolean _isMapping;

	public ProvIdbNode(String label, String semiringName, boolean isMapping) {
		super(semiringName);
		_label = label;
		_isMapping = isMapping;
	}

	public ProvIdbNode(String label, List<ProvenanceNode> children, String semiringName, boolean isMapping) {
		super(children, semiringName);
		_label = label;
		_isMapping = isMapping;
	}

	public String getLabel() {
		return _label;
	}

	public String toString() {
		StringBuffer str = new StringBuffer(_label + "(");

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				str.append("*");
			str.append(p.toString());
			first = false;
		}
		str.append(")");

		return new String(str);
	}

	public ProvIdbNode copySelf() {
		return new ProvIdbNode(_label, _semiringName, _isMapping);
	}

	public List<Object> getStringExpr() {
		List<Object> results = new ArrayList<Object>();

		if(_isMapping)
			results.add(_label + "(");
		else
			results.add("(");

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				results.add("*");
			results.addAll(p.getStringExpr());
			first = false;
		}
		results.add(")");

		simplify(results);

		return results;
	}

	public List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException {
		List<Object> results = new ArrayList<Object>();

		//		results.add(_labelValue + "" + multiOp + "(");
		results.add("(");

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				results.add(multiOp(_semiringName));
			results.addAll(p.getValueExpr(rule));
			first = false;
		}
		results.add(")");
		if(first){
			Debug.println(getLabel() + " IDB NODE WITH NO CHILDREN?!");
		}
		//		simplify(results);

		return results;
	}

}
