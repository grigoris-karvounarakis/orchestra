package edu.upenn.cis.orchestra.optimization;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class ViewSummary {
	final Map<String,Integer> relAtoms;
	final boolean aggregated;

	private final int hashCode;

	ViewSummary(Expression exp) {
		this(exp.relAtoms, exp.groupBy != null);
	}
	
	ViewSummary(Map<String,Integer> relAtoms, boolean aggregated) {
		this.relAtoms = Collections.unmodifiableMap(new HashMap<String,Integer>(relAtoms));
		this.aggregated = aggregated;
		hashCode = relAtoms.hashCode() + (aggregated ? 37 : 0);
		
	}

	public int hashCode() {
		return hashCode;
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		ViewSummary vs = (ViewSummary) o;
		return (aggregated == vs.aggregated && relAtoms.equals(vs.relAtoms));
	}		
}