package edu.upenn.cis.orchestra.p2pqp;


class TupleAndNodeIds {
	final QpTuple<Null> t;

	<M> TupleAndNodeIds(QpTuple<M> t) {
		this.t = t.changeMetadata((Null) null, null);
	}
	
	public int hashCode() {
		return t.hashCode() + 37 * t.contributingNodesHashcode();
	}
	
	public boolean equals(Object o) {
		TupleAndNodeIds tani = (TupleAndNodeIds) o;
		return t.sameContributingNodes(tani.t) && t.equals(tani.t);
	}
}
