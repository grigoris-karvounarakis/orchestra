package edu.upenn.cis.orchestra.optimization;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Optimizer.ExpIdAndMorphism;

/**
 * Create certain aggregate variables by grouping on the specified variables
 * 
 * @author netaylor
 *
 */
class AggregateNode extends OneInputNode {
	final Set<Aggregate> aggregates;
	final Set<Variable> groupingVariables;
	
	AggregateNode(ExpIdAndMorphism input, Set<Aggregate> aggregates, Set<Variable> groupingVariables) {
		super(input);
		this.aggregates = aggregates;
		this.groupingVariables = groupingVariables;
	}

	AggregateNode(ExpIdAndMorphism input, Set<Predicate> preds, ExpIdAndMorphism beforePreds, Set<Aggregate> aggregates, Set<Variable> groupingVariables) {
		super(input, preds, beforePreds);
		this.aggregates = aggregates;
		this.groupingVariables = groupingVariables;
	}

	@Override
	AndNodeType getNodeType() {
		return AndNodeType.AGG;
	}
}
