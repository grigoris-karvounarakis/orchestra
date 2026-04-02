package edu.upenn.cis.orchestra.optimization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class GenericHistogram<T extends Comparable<? super T> & Serializable> extends Histogram<T> implements Serializable {
	private static final long serialVersionUID = 1L;
	
	
	// Each bucket includes its lower edge but not its upper edge
	final List<T> bucketEdges;

	GenericHistogram(List<T> bucketEdges, double[] cardinality, double[] distinctVals, TypeOps<T> gbs) {
		super(gbs, cardinality, distinctVals);
		this.bucketEdges = Collections.unmodifiableList(new ArrayList<T>(bucketEdges));
		checkValid();
	}
	
	public List<T> getBucketEdges() {
		return bucketEdges;
	}


	protected Histogram<T> createNewHistogram(List<T> bucketEdges, double[] cardinality,
			double[] distinctVals) {
		return new GenericHistogram<T>(bucketEdges, cardinality, distinctVals, typeOps);
	}


	public boolean isEmpty() {
		return bucketEdges.isEmpty();
	}

}
