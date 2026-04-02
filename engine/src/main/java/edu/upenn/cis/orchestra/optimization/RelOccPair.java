package edu.upenn.cis.orchestra.optimization;

class RelOccPair {
	String relation;
	int occ;

	RelOccPair(String relation, int pos) {
		this.relation = relation;
		this.occ = pos;
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		RelOccPair rp = (RelOccPair) o;

		return (relation.equals(rp.relation) && occ == rp.occ);
	}

	public int hashCode() {
		return occ + 37 * relation.hashCode();
	}

	public String toString() {
		return "[" + relation + "," + occ + "]";
	}
}

class RelOccPairs {
	final String rel1, rel2;
	final int occ1, occ2;
	
	RelOccPairs(String rel1, String rel2, int occ1, int occ2) {
		this.rel1 = rel1;
		this.rel2 = rel2;
		this.occ1 = occ1;
		this.occ2 = occ2;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		RelOccPairs rop = (RelOccPairs) o;
		
		return (rel1.equals(rop.rel1) && rel2.equals(rop.rel2) && occ1 == rop.occ1 && occ2 == rop.occ2);
	}
	
	public int hashCode() {
		return rel1.hashCode() + 37 * rel2.hashCode() +  61 * occ1 + 127 * occ2;
	}
}