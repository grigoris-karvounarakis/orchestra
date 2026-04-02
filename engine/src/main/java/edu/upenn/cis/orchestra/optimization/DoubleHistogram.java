package edu.upenn.cis.orchestra.optimization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

class DoubleHistogram extends Histogram<Double> implements Serializable {
	private static final long serialVersionUID = 1L;
	private final double[] bucketEdges;

	public DoubleHistogram(List<Double> bucketEdges, double[] cardinality,
			double[] distinctVals, TypeOps<Double> gbs) {
		super(gbs, cardinality, distinctVals);

		this.bucketEdges = new double[bucketEdges.size()];
		Iterator<Double> it = bucketEdges.iterator();
		int pos = 0;
		while (it.hasNext()) {
			this.bucketEdges[pos++] = it.next();
		}
		checkValid();
	}

	protected Histogram<Double> createNewHistogram(List<Double> bucketEdges,
			double[] cardinality, double[] distinctVals) {
		return new DoubleHistogram(bucketEdges, cardinality, distinctVals, this.typeOps);
	}

	public List<Double> getBucketEdges() {
		List<Double> retval = new ArrayList<Double>(bucketEdges.length);
		
		for (int i = 0; i < bucketEdges.length; ++i) {
			retval.add(bucketEdges[i]);
		}
		
		return retval;
	}

	public boolean isEmpty() {
		return bucketEdges.length == 0;
	}

}
