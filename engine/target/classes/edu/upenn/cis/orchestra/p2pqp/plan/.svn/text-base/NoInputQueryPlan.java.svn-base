package edu.upenn.cis.orchestra.p2pqp.plan;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class NoInputQueryPlan<M> extends QueryPlan<M> {
	public NoInputQueryPlan(String outputSchema, Location loc,
			int operatorId, boolean isReplicated) {
		super(outputSchema, loc, operatorId, getDescendents(), isReplicated);
	}

	final public Iterator<QueryPlanAndDest<M>> iterator() {
		List<QueryPlanAndDest<M>> empty = Collections.emptyList();
		return empty.iterator();
	}
		
	private static Set<Integer> getDescendents() {
		return Collections.emptySet();
	}
	
	public void getShipInputLocations(Map<OperatorAndDest,Location> shipInputLocs) {	
	}
}
