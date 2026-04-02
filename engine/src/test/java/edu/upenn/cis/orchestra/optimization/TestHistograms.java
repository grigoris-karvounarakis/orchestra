package edu.upenn.cis.orchestra.optimization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import edu.upenn.cis.orchestra.datamodel.Date;
import java.util.List;

import org.junit.Before;
import org.junit.Test;

import edu.upenn.cis.orchestra.optimization.Histogram.Results;

public class TestHistograms {

	Histogram<Integer> intHist, intHist2;
	Histogram<Double> doubleHist;
	Histogram<String> stringHist;
	Histogram<Date> dateHist;
	Histogram<Integer> emptyHist;

	static final Date d1 = new Date(1982,1,1), d2 = new Date(1983,1,1),
	d3 = new Date(1984,1,1), d1b = new Date(1982,7,1);

	@Before
	public void setUp() {
		double[] cardinality = {4.0, 3.0};
		double[] distinctValues = {2.0, 3.0};
		double[] cardinality2 = {4.0, 1.0, 2.0};
		double[] distinctValues2 = {3.0, 1.0, 2.0};
		double[] cardinality3 = {2.0, 1.0, 4.0};
		double[] distinctValues3 = {1.0, 1.0, 3.0};

		List<Integer> intBucketEdges = new ArrayList<Integer>();
		intBucketEdges.add(0);
		intBucketEdges.add(2);
		intBucketEdges.add(6);

		List<Integer> intBucketEdges2 = new ArrayList<Integer>();
		intBucketEdges2.add(1);
		intBucketEdges2.add(2);
		intBucketEdges2.add(4);
		intBucketEdges2.add(7);

		intHist = Histogram.createIntegerHistogram(intBucketEdges, cardinality, distinctValues);
		intHist2 = Histogram.createIntegerHistogram(intBucketEdges2, cardinality3, distinctValues3);

		List<Double> doubleBucketEdges = new ArrayList<Double>();
		doubleBucketEdges.add(0.0);
		doubleBucketEdges.add(2.0);
		doubleBucketEdges.add(6.0);

		doubleHist = Histogram.createDoubleHistogram(doubleBucketEdges, cardinality, distinctValues);


		List<String> stringBucketEdges = new ArrayList<String>();
		stringBucketEdges.add(Histogram.convertForHistogram(3, "Alfred"));
		stringBucketEdges.add(Histogram.convertForHistogram(3, "Lewis"));
		stringBucketEdges.add(Histogram.getSuccessorForHistogram(3, "Lewis"));
		stringBucketEdges.add(Histogram.getSuccessorForHistogram(3, "Xavier"));

		stringHist = Histogram.createStringHistogram(3, stringBucketEdges, cardinality2, distinctValues2);

		List<Integer> empty = Collections.emptyList();

		emptyHist = Histogram.createIntegerHistogram(empty, new double[0], new double[0]);

		List<Date> dateBucketEdges = new ArrayList<Date>();
		dateBucketEdges.add(d1);
		dateBucketEdges.add(d2);
		dateBucketEdges.add(d3);

		dateHist = Histogram.createDateHistogram(dateBucketEdges, cardinality, distinctValues);
	}

	@Test
	public void testIntHistogramBuckets() {
		Results r1 = intHist.getNumInRange(0, 1),
		r2 = intHist.getNumInRange(2, 5);

		assertEquals("Wrong cardinality of first bucket", 4.0, r1.cardinality, 0.0001);
		assertEquals("Wrong cardinality of second bucket", 3.0, r2.cardinality, 0.0001);

		assertEquals("Wrong number of distinct values in first bucket", 2.0, r1.distinctValues, 0.0001);
		assertEquals("Wrong number of distinct values in second bucket", 3.0, r2.distinctValues, 0.0001);
	}

	@Test
	public void testIntHistogramInterpolation() {
		Results r = intHist.getNumInRange(0, 3);

		assertEquals("Wrong cardinality", 5.5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values", 3.5, r.distinctValues, 0.0001);
	}

	@Test
	public void testIntHistogramRemoval() {
		Histogram<Integer> h1 = intHist.removeRange(0, 1);
		List<Integer> buckets = h1.getBucketEdges();
		assertEquals("Wrong number of buckets", 1, buckets.size() - 1);
		assertEquals("Wrong bucket bottom", 2, buckets.get(0));
		assertEquals("Wrong bucket top", 6, buckets.get(1));

		Results r = h1.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 3.0, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 3.0, r.distinctValues, 0.0001);

		Histogram<Integer> h2 = intHist.removeRange(1, 3);
		buckets = h2.getBucketEdges();
		assertEquals("Wrong number of buckets", 3, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", 0, buckets.get(0));
		assertEquals("Wrong second bucket bottom", 1, buckets.get(1));
		assertEquals("Wrong third bucket bottom", 4, buckets.get(2));
		assertEquals("Wrong third bucket top", 6, buckets.get(3));

		r = h2.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 3.5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 2.5, r.distinctValues, 0.0001);

		Histogram<Integer> h3 = intHist.removeRange(0, 0);
		buckets = h3.getBucketEdges();
		assertEquals("Wrong number of buckets", 2, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", 1, buckets.get(0));
		assertEquals("Wrong second bucket bottom", 2, buckets.get(1));
		assertEquals("Wrong second bucket top", 6, buckets.get(2));

		r = h3.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 4, r.distinctValues, 0.0001);
	}

	@Test
	public void testIntHistogramRetain() {
		Histogram<Integer> h1 = intHist.retainRange(1, 3);
		List<Integer> buckets = h1.getBucketEdges();
		assertEquals("Wrong number of buckets", 2, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", 1, buckets.get(0));
		assertEquals("Wrong second bucket bottom", 2, buckets.get(1));
		assertEquals("Wrong second bucket top", 4, buckets.get(2));

		Results r = h1.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 3.5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 2.5, r.distinctValues, 0.0001);

		Histogram<Integer> h2 = intHist.retainRange(3, 4);
		buckets = h2.getBucketEdges();
		assertEquals("Wrong number of buckets", 1, buckets.size() - 1);
		assertEquals("Wrong bucket bottom", 3, buckets.get(0));
		assertEquals("Wrong bucket top", 5, buckets.get(1));
		r = h2.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 1.5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 1.5, r.distinctValues, 0.0001);

		Histogram<Integer> h3 = intHist.retainRange(0, 1);
		buckets = h3.getBucketEdges();
		assertEquals("Wrong number of buckets", 1, buckets.size() - 1);
		assertEquals("Wrong bucket bottom", 0, buckets.get(0));
		assertEquals("Wrong bucket top", 2, buckets.get(1));
		r = h3.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 4, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 2, r.distinctValues, 0.0001);
	}

	@Test
	public void testDoubleHistogramRetrieve() {
		Results r1 = doubleHist.getNumInRange(0.0, 2.0),
		r2 = doubleHist.getNumInRange(2.0, 6.0);

		assertEquals("Wrong cardinality of first bucket", 4.0, r1.cardinality, 0.0001);
		assertEquals("Wrong cardinality of second bucket", 3.0, r2.cardinality, 0.0001);

		assertEquals("Wrong number of distinct values in first bucket", 2.0, r1.distinctValues, 0.0001);
		assertEquals("Wrong number of distinct values in second bucket", 3.0, r2.distinctValues, 0.0001);

		Results r3 = doubleHist.getNumInRange(0.0, 1.0),
		r4 = doubleHist.getNumInRange(1.0, 4.0);

		assertEquals("Wrong cardinality", 2.0, r3.cardinality, 0.0001);
		assertEquals("Wrong distinct values", 1.0, r3.distinctValues, 0.0001);
		assertEquals("Wrong cardinality", 3.5, r4.cardinality, 0.0001);
		assertEquals("Wrong distinct values", 2.5, r4.distinctValues, 0.0001);
	}

	@Test
	public void testDoubleHistogramRemoval() {
		Histogram<Double> h1 = doubleHist.removeRange(1.0, 4.0);

		List<Double> buckets = h1.getBucketEdges();
		assertEquals("Wrong number of buckets", 3, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", 0.0, buckets.get(0), 0.0001);
		assertEquals("Wrong second bucket bottom", 1.0, buckets.get(1), 0.0001);
		assertEquals("Wrong third bucket bottom", 4.0, buckets.get(2), 0.0001);
		assertEquals("Wrong third bucket top", 6.0, buckets.get(3), 0.0001);

		Results r = h1.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 3.5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 2.5, r.distinctValues, 0.0001);
	}

	@Test
	public void testDoubleHistogramRetain() {
		Histogram<Double> h1 = doubleHist.retainRange(1.0, 4.0);

		List<Double> buckets = h1.getBucketEdges();
		assertEquals("Wrong number of buckets", 2, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", 1.0, buckets.get(0), 0.0001);
		assertEquals("Wrong second bucket bottom", 2.0, buckets.get(1), 0.0001);
		assertEquals("Wrong second bucket top", 4.0, buckets.get(2), 0.0001);

		Results r = h1.getNumInRange(null, null);
		assertEquals("Wrong cardinality in derived histogram", 3.5, r.cardinality, 0.0001);
		assertEquals("Wrong distinct values in derived histogram", 2.5, r.distinctValues, 0.0001);
	}

	@Test
	public void testStringHistogramConvert() {
		String converted = Histogram.convertForHistogram(5, "N. Taylor");
		assertEquals("Incorrect normalization for string histogram", "N  TA", converted);

		String successor = Histogram.getSuccessorForHistogram(5, "N. TAY");
		assertEquals("Incorrect successor normalization for string histogram", "N  TB", successor);

		String pred = Histogram.getPredecessorForHistogram(3, "ZA?");
		assertEquals("Incorrect predecessor normalization for string histogram", "Z9Z", pred);
	}

	@Test
	public void testStringHistogramRetrieve() {
		Results r = stringHist.getNumInRange("Lewis", "Lewis");
		assertEquals("Wrong cardinality results for single bucket", 1.0, r.cardinality);
		assertEquals("Wrong distinct value results for single bucket", 1.0, r.distinctValues);

		Results r2 = stringHist.getNumInRange(null, "Lewis");
		assertEquals("Wrong cardinality results for two buckets", 5.0, r2.cardinality);
		assertEquals("Wrong distinct value results for two buckets", 4.0, r2.distinctValues);

		Results r3 = stringHist.getNumInRange("James", "Peter");
		assertEquals("Wrong cardinality results from interpolation", 3 * 4.0 / 12 + 1.0 + 5 * 2.0 / 13, r3.cardinality, 0.5);
		assertEquals("Wrong distinct value results from interpolation", 3 * 3.0 / 12 + 1.0 + 5 * 2.0 / 13, r3.distinctValues, 0.5);
	}

	@Test
	public void testStringHistogramRetain() {
		Histogram<String> h1 = stringHist.retainRange("James", "Peter");
		List<String> buckets = h1.getBucketEdges();

		assertEquals("Wrong number of buckets", 3, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", "JAM", buckets.get(0));
		assertEquals("Wrong second bucket bottom", "LEW", buckets.get(1));
		assertEquals("Wrong third bucket bottom", "LEX", buckets.get(2));
		assertEquals("Wrong third bucket top", "PEU", buckets.get(3));

		Results r1 = h1.getNumInRange(null, null);
		assertEquals("Wrong cardinality results from derived histogram", 3 * 4.0 / 12 + 1.0 + 5 * 2.0 / 13, r1.cardinality, 0.5);
		assertEquals("Wrong distinct value results from derived histogram", 3 * 3.0 / 12 + 1.0 + 5 * 2.0 / 13, r1.distinctValues, 0.5);

		Results r2 = h1.getNumInRange("ARK", "LEV");
		assertEquals("Wrong cardinality results from derived histogram", 3 * 4.0 / 12, r2.cardinality, 0.5);
		assertEquals("Wrong distinct value results from derived histogram", 3 * 3.0 / 12, r2.distinctValues, 0.5);
	}

	@Test
	public void testStringHistogramRemove() {
		Histogram<String> h1 = stringHist.removeRange("James", "Peter");
		List<String> buckets = h1.getBucketEdges();
		assertEquals("Wrong number of buckets", 3, buckets.size() - 1);
		assertEquals("Wrong first bucket bottom", "ALF", buckets.get(0));
		assertEquals("Wrong second bucket bottom", "JAM", buckets.get(1));
		assertEquals("Wrong third bucket bottom", "PEU", buckets.get(2));
		assertEquals("Wrong third bucket top", "XAW", buckets.get(3));

		Results r1 = h1.getNumInRange(null, null);
		assertEquals("Wrong cardinality results from derived histogram", 9 * 4.0 / 12 + 8 * 2.0 / 13, r1.cardinality, 0.5);
		assertEquals("Wrong distinct value results from derived histogram", 9 * 3.0 / 12 + 8 * 2.0 / 13, r1.distinctValues, 0.5);

		Results r2 = h1.getNumInRange("James", "Peter");
		assertEquals("Wrong cardinality for removed range", 0.0, r2.cardinality);
		assertEquals("Wrong distinct values for removed range", 0.0, r2.distinctValues);

		Results r3 = h1.getNumInRange("ALF", "LEW");
		assertEquals("Wrong cardinality results from derived histogram", 9 * 4.0 / 12, r3.cardinality, 0.5);
		assertEquals("Wrong distinct value results from derived histogram", 9 * 3.0 / 12, r3.distinctValues, 0.5);
	}

	@Test
	public void testEmptyHistogramRetrieve() {
		assertTrue("Wrong number of buckets in empty histogram", emptyHist.getBucketEdges().isEmpty());

		Results r = emptyHist.getNumInRange(null, null);
		assertEquals("Wrong cardinality of empty histogram", 0.0, r.cardinality);
		assertEquals("Wrong number of distinct values in empty histogram", 0.0, r.distinctValues);
	}

	@Test
	public void testDateHistogramRetrieve() {
		Results r1 = dateHist.getNumInRange(d1b, d3);
		assertEquals("Wrong cardinality from date histogram", 5.0, r1.cardinality, 0.05);
		assertEquals("Wrong number of distinct values in date histogram", 4.0, r1.distinctValues, 0.05);
	}

	@Test
	public void testHistogramScale() {
		Histogram<Integer> h1 = intHist.scaleHistogram(2.0, 0.5);
		Results r1 = h1.getNumInRange(0, 1);
		assertEquals("Wrong cardinality from scaled histogram", 8.0, r1.cardinality, 0.0001);
		assertEquals("Wrong number of distinct values in scaled histogram", 1.0, r1.distinctValues);
	}

	@Test
	public void testHistogramUnion() {
		Histogram<Integer> h1 = intHist.union(intHist2);

		List<Integer> buckets = h1.getBucketEdges();

		List<Integer> expectedBucks = new ArrayList<Integer>();
		expectedBucks.add(0);
		expectedBucks.add(1);
		expectedBucks.add(2);
		expectedBucks.add(4);
		expectedBucks.add(6);
		expectedBucks.add(7);
		assertEquals("Wrong buckets in derived histogram", expectedBucks, buckets);

		Results r0 = h1.getNumInRange(0, 0);
		assertEquals("Wrong cardinality from derived histogram", 2.0, r0.cardinality, 0.0001);
		assertEquals("Wrong number of distinct values in scaled histogram", 1.0, r0.distinctValues, 0.0001);

		Results r1 = h1.getNumInRange(2, 5);
		assertEquals("Wrong cardinality from derived histogram", 6.6666667, r1.cardinality, 0.0001);
		assertEquals("Wrong number of distinct values in scaled histogram", 3.75, r1.distinctValues, 0.0001);
	}

	@Test
	public void testHistogramJoin() {
		Histogram<Integer> h1 = intHist.joinWith(intHist2);

		List<Integer> buckets = h1.getBucketEdges();

		List<Integer> expectedBucks = new ArrayList<Integer>();
		expectedBucks.add(0);
		expectedBucks.add(1);
		expectedBucks.add(2);
		expectedBucks.add(4);
		expectedBucks.add(6);
		expectedBucks.add(7);
		assertEquals("Wrong buckets in derived histogram", expectedBucks, buckets);

		Results r0 = h1.getNumInRange(0, 0);
		assertEquals("Wrong cardinality from derived histogram", 0.0, r0.cardinality, 0.0001);
		assertEquals("Wrong number of distinct values in scaled histogram", 0.0, r0.distinctValues, 0.0001);

		Results r1 = h1.getNumInRange(2, 3);
		assertEquals("Wrong cardinality from derived histogram", 0.75, r1.cardinality, 0.0001);
		assertEquals("Wrong number of distinct values in scaled histogram", 0.75, r1.distinctValues, 0.0001);

		Results r2 = h1.getNumInRange(4, 5);
		assertEquals("Wrong cardinality from derived histogram", 2.0, r2.cardinality, 0.0001);
		assertEquals("Wrong number of distinct values in scaled histogram", 1.5, r2.distinctValues, 0.0001);
	}
	
	@Test
	public void testHistogramCount() {
		Histogram<Integer> h1 = Histogram.createCountHistogram(intHist.getNumInRange(null, null).cardinality, 3);
		Results total = h1.getNumInRange(null, null);
		
		assertEquals("Wrong total count", 3.0, total.cardinality, 0.0001);
	}
	
	@Test
	public void testRetainCreatesEmpty() {
		Histogram<Integer> h1 = intHist.retainRange(null, -3);
		assertTrue("Should be empty", h1.isEmpty());
		Histogram<Integer> h2 = intHist.retainRange(75, null);
		assertTrue("Should be empty", h2.isEmpty());
		Histogram<Integer> h3 = intHist.retainRange(75, 105);
		assertTrue("Should be empty", h3.isEmpty());
	}
}
