package edu.upenn.cis.orchestra.optimization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


class IntHistogram extends Histogram<Integer> implements Serializable {
	private static final long serialVersionUID = 1L;
	private final int[] bucketEdges;

	public IntHistogram(List<Integer> bucketEdges, double[] cardinality,
			double[] distinctVals, TypeOps<Integer> gbs) {
		super(gbs, cardinality, distinctVals);

		this.bucketEdges = new int[bucketEdges.size()];
		Iterator<Integer> it = bucketEdges.iterator();
		int pos = 0;
		while (it.hasNext()) {
			this.bucketEdges[pos++] = it.next();
		}
		checkValid();
	}

	protected Histogram<Integer> createNewHistogram(List<Integer> bucketEdges,
			double[] cardinality, double[] distinctVals) {
		return new IntHistogram(bucketEdges, cardinality, distinctVals, this.typeOps);
	}

	public List<Integer> getBucketEdges() {
		List<Integer> retval = new ArrayList<Integer>(bucketEdges.length);
		
		for (int i = 0; i < bucketEdges.length; ++i) {
			retval.add(bucketEdges[i]);
		}
		
		return retval;
	}

	public boolean isEmpty() {
		return bucketEdges.length == 0;
	}

}
