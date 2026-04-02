package edu.upenn.cis.orchestra.optimization;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Optimizer.ExpIdAndMorphism;

/**
 * Join two expressions based on the equality of the list of
 * attributes for one expression with the list of attributes
 * for the second expression.
 * 
 * @author netaylor
 *
 */
class JoinNode extends AndNode {
	final ExpIdAndMorphism lhs, rhs;
	// Lists of join variables. The names used are those
	// used in the left and right child, respectively
	final List<Variable> lhsJoinVars, rhsJoinVars;
	// Mapping from the child expressions into this expression
	final Morphism lhsMap, rhsMap;
	
	JoinNode(ExpIdAndMorphism lhs, ExpIdAndMorphism rhs, List<Variable> lhsJoinVars, List<Variable> rhsJoinVars,
			Morphism lhsMap, Morphism rhsMap) {
		this.lhs = lhs;
		this.rhs = rhs;
		this.lhsMap = lhsMap;
		this.rhsMap = rhsMap;
		
		if (lhsJoinVars.size() != rhsJoinVars.size()) {
			throw new IllegalArgumentException("Lists of join variables not of equal length");
		}
		
		if (lhsJoinVars.isEmpty()) {
			throw new IllegalArgumentException("Should not have empty set of join variables");
		}
		
		this.lhsJoinVars = lhsJoinVars;
		this.rhsJoinVars = rhsJoinVars;
	}

	JoinNode(Set<Predicate> preds, ExpIdAndMorphism beforePreds, ExpIdAndMorphism lhs, ExpIdAndMorphism rhs, List<Variable> lhsJoinVars, List<Variable> rhsJoinVars,
			Morphism lhsMap, Morphism rhsMap) {
		super(preds,beforePreds);
		this.lhs = lhs;
		this.rhs = rhs;
		this.lhsMap = lhsMap;
		this.rhsMap = rhsMap;
		
		if (lhsJoinVars.size() != rhsJoinVars.size()) {
			throw new IllegalArgumentException("Lists of join variables not of equal length");
		}
		
		this.lhsJoinVars = lhsJoinVars;
		this.rhsJoinVars = rhsJoinVars;
	}
	
	@Override
	List<ExpIdAndMorphism> getInputs() {
		List<ExpIdAndMorphism> retval = new ArrayList<ExpIdAndMorphism>(2);
		retval.add(lhs);
		retval.add(rhs);
		return retval;
	}

	@Override
	AndNodeType getNodeType() {
		return AndNodeType.JOIN;
	}	
}
