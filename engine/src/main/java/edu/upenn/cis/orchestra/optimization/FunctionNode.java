package edu.upenn.cis.orchestra.optimization;

import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Optimizer.ExpIdAndMorphism;


class FunctionNode extends OneInputNode {
	final Set<Function> functions;
	
	FunctionNode(ExpIdAndMorphism input, Set<Function> functions) {
		super(input);
		this.functions = functions;
	}

	FunctionNode(ExpIdAndMorphism input, Set<Predicate> preds, ExpIdAndMorphism beforePreds, Set<Function> functions) {
		super(input,preds,beforePreds);
		this.functions = functions;
	}
	@Override
	AndNodeType getNodeType() {
		return AndNodeType.FUNC;
	}
}

