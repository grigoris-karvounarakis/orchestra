package edu.upenn.cis.orchestra.optimization;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;


public abstract class DoubleCostQueryPlanGenerator<P extends PhysicalProperties, S extends AbstractRelation, QP> implements QueryPlanGenerator<P, Double, S, QP> {	
	public Double addTogether(Double c1, Double c2) {
		return c1 + c2;
	}
	
	public Double subtractFrom(Double c1, Double c2) {
		return c1 - c2;
	}
	
	public Double getIdentity() {
		return 0.0;
	}
	
	public int compare(Double o1, Double o2) {
		return o1.compareTo(o2);
	}
}
