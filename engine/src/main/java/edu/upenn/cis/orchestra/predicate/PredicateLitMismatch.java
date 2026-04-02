package edu.upenn.cis.orchestra.predicate;

import edu.upenn.cis.orchestra.datamodel.Type;

public class PredicateLitMismatch extends Exception {
	private static final long serialVersionUID = 1L;
	
	PredicateLitMismatch(Type t, Object o) {
		super("Attempt to create predicate comparing value of types " + 
				t + " and " + o.getClass().getName());
	}
}
