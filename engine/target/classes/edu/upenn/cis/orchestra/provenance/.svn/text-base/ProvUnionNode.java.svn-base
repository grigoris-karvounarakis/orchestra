package edu.upenn.cis.orchestra.provenance;

import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.Debug;
import edu.upenn.cis.orchestra.mappings.Rule;
import edu.upenn.cis.orchestra.provenance.exceptions.InvalidAssignmentException;

public class ProvUnionNode extends ProvenanceNode {

	public ProvUnionNode(String semiringName){
		super(semiringName);
	}

	public String toString() {
		StringBuffer str = new StringBuffer();

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				str.append("+");
			str.append(p.toString());
			first = false;
		}

		return new String(str);
	}

	public ProvUnionNode copySelf() {
		return new ProvUnionNode(_semiringName);
	}

	public List<Object> getStringExpr() {
		List<Object> results = new ArrayList<Object>();

		boolean first = true;
		for (ProvenanceNode p : getChildren()) {
			if (!first)
				results.add("+");
			results.addAll(p.getStringExpr());
			first = false;
		}

		simplify(results);

		return results;
	}

	public List<Object> getValueExpr(Rule rule) throws InvalidAssignmentException {
		List<Object> results = new ArrayList<Object>();
		String plusOp;

		//		Max needs Polish notation and   
		if(RANK_SEMIRING.equalsIgnoreCase(_semiringName) || TROPICAL_MAX_SEMIRING.equalsIgnoreCase(_semiringName) || TROPICAL_MIN_SEMIRING.equalsIgnoreCase(_semiringName)){
			if(TROPICAL_MAX_SEMIRING.equalsIgnoreCase(_semiringName))
				plusOp = "MAX";
			else
				plusOp = "MIN";

			//		results.add(_labelValue + "" + multiOp + "(");
			if(getChildren().size() > 1)
				results.add(plusOp);
			results.add("(");

			boolean first = true;
			for (ProvenanceNode p : getChildren()) {
				if (!first)
					results.add(", ");
				results.addAll(p.getValueExpr(rule));
				first = false;
			}
			results.add(")");
			if(first){
				Debug.println("NO CHILDREN?!");
			}

		}else{
			if(TRUST_SEMIRING.equalsIgnoreCase(_semiringName) || BAG_SEMIRING.equalsIgnoreCase(_semiringName))
				plusOp = "+";
			else
				plusOp = "+";

			boolean first = true;
			for (ProvenanceNode p : getChildren()) {
				if (!first)
					results.add(plusOp);
				results.addAll(p.getValueExpr(rule));
				first = false;
			}
		}		
		
		simplify(results);

		return results;
	}

}
