package edu.upenn.cis.orchestra.optimization;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Optimizer.ExpIdAndMorphism;

class ViewNode extends AndNode {
	final String viewName;
	final Morphism viewToThis;
	
	public ViewNode(String viewName, Morphism viewToThis) {
		this.viewName = viewName;
		this.viewToThis = viewToThis;
	}

	public ViewNode(Set<Predicate> predicates, ExpIdAndMorphism beforePreds, String viewName, Morphism viewToThis) {
		super(predicates, beforePreds);
		this.viewName = viewName;
		this.viewToThis = viewToThis;
	}

	@Override
	List<ExpIdAndMorphism> getInputs() {
		return Collections.emptyList();
	}

	@Override
	AndNodeType getNodeType() {
		return AndNodeType.VIEW;
	}

}
