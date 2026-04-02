package edu.upenn.cis.orchestra.optimization;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.optimization.Optimizer.ExpIdAndMorphism;

abstract class AndNode {
	abstract List<ExpIdAndMorphism> getInputs();
	
	final Set<Predicate> predicates;
	final ExpIdAndMorphism beforePreds;
	
	AndNode() {
		this.predicates = null;
		this.beforePreds = null;
	}
	
	AndNode(Set<Predicate> predicates, ExpIdAndMorphism beforePreds) {
		if (predicates == null || beforePreds == null) {
			throw new NullPointerException();
		}
		if (predicates.isEmpty()) {
			throw new IllegalArgumentException("Predicate must be a non-empty set");
		}
		this.predicates = Collections.unmodifiableSet(new HashSet<Predicate>(predicates));
		this.beforePreds = beforePreds;
	}
	
	enum AndNodeType {
		FUNC, AGG, JOIN, SCAN, VIEW;
		
		private Set<AndNodeType> following;
		
		Set<AndNodeType> getFollowing() {
			if (following == null) {
				Set<AndNodeType> f = new HashSet<AndNodeType>();
				
				if (this == FUNC) {
					f.add(AGG);
					f.add(JOIN);
					f.add(SCAN);
					f.add(VIEW);
				} else if (this == AGG) {
					f.add(FUNC);
					f.add(JOIN);
					f.add(SCAN);
					f.add(VIEW);
					f.add(AGG);
				} else if (this == JOIN) {
					f.add(FUNC);
					f.add(AGG);
					f.add(JOIN);
					f.add(SCAN);
					f.add(VIEW);
				} else if (this == VIEW) {
					throw new IllegalStateException("Should never try to determine what follows a view");
				} else {
					throw new IllegalStateException("Should never try to determine what follows a scan");
				}
				
				following = Collections.unmodifiableSet(f);
			}
			return following;
		}
		
		public static final List<AndNodeType> all = Collections.unmodifiableList((Arrays.asList(AndNodeType.values())));
	}
	
	abstract AndNodeType getNodeType();
}


abstract class OneInputNode extends AndNode {
	final ExpIdAndMorphism input;

	OneInputNode(ExpIdAndMorphism input) {
		this.input = input;
	}
	
	OneInputNode(ExpIdAndMorphism input, Set<Predicate> preds, ExpIdAndMorphism beforePreds) {
		super(preds, beforePreds);
		this.input = input;
	}
	
	List<ExpIdAndMorphism> getInputs() {
		return Collections.singletonList(input);
	}
}