package edu.upenn.cis.orchestra.p2pqp;


public abstract class TupleProvenance {
	int creator;
	
	public int getCreator() {
		return creator;
	}
	
	public void setCreator(int operatorid) {
		creator = operatorid;
	}
	 
	public boolean equals(TupleProvenance t) {
		return equalProvenance(t);
	}
	public abstract byte[] getBytes();
	
	/**
	 * Extend the provenance graph with a new derivation
	 * 
	 * @param op
	 * @return
	 */
	public abstract TupleProvenance derive(int op);
	
	/**
	 * Return a new provenance expression in which we are joined with the second expr. 
	 * @param r_2
	 * @return
	 */
	public abstract TupleProvenance joinWith(TupleProvenance r_2, int operator);
	public abstract TupleProvenance unionWith(TupleProvenance r_2, int operator);
	
	/**
	 * Subtract the second provenance expression
	 * @param r_2
	 * @return
	 */
	public abstract TupleProvenance differenceWith (TupleProvenance r_2);
    public abstract boolean contains(TupleProvenance r_2);
    public abstract TupleProvenance find(TupleProvenance r_2);
	/*
	public abstract long applyFunctions(Statistics.Func aggFunc, Statistics.Func tuplePv);
	public TupleProvenance TupleProvenanceSelect(TupleProvenance p){
		return new TupleProvenance(absorbProv.restrict(p.absorbProv));
	}
	public TupleProvenance TupleProvenanceProject(TupleProvenance v){
		return new TupleProvenance(absorbProv.exist(v.absorbProv));
	}
	public TupleProvenance TupleProvenanceJoinProject(TupleProvenance r_2, 
			TupleProvenance v){
		return new TupleProvenance(absorbProv.relprod(r_2.absorbProv, v.absorbProv));
	}
	public TupleProvenance TupleProvenanceRename(BDDPairing p){
		return new TupleProvenance(absorbProv.replace(p));
	}*/
	public abstract TupleProvenance copy();
	
	public abstract boolean equalProvenance(TupleProvenance r_2);

	public abstract String toString();
	
	public abstract int size();
	
	public abstract TupleProvenance setZero(TupleProvenance p);
	
	public abstract boolean isZero();

//	public abstract boolean isOne();
}
