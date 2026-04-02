package edu.upenn.cis.orchestra.optimization;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import edu.upenn.cis.orchestra.datamodel.Date;
import java.util.Iterator;
import java.util.List;

import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;

/**
 * @author netaylor
 *
 * @param <T>
 */
public abstract class Histogram<T extends Comparable<? super T> & Serializable> implements Serializable {
	private static final long serialVersionUID = 1L;

	protected final TypeOps<T> typeOps;

	private final double[] cardinality;
	private final double[] distinctVals;


	protected Histogram(TypeOps<T> typeOps, double[] cardinality, double[] distinctVals) {
		this.typeOps = typeOps;
		this.cardinality = cardinality;
		this.distinctVals = distinctVals;
	}

	protected void checkValid() {
		List<T> bucketEdges = getBucketEdges();
		if (bucketEdges.isEmpty()) {
			if (cardinality.length != 0 || distinctVals.length != 0) {
				throw new IllegalArgumentException("If bucketEdges is empty all inputs must be empty");
			}
		} else {
			if (bucketEdges.size() != (cardinality.length + 1) || cardinality.length != distinctVals.length) {
				throw new IllegalArgumentException("Must have one more bucket edge than cardinalities and distinct values");
			}
		}

		T prev = null;
		for (T val : bucketEdges) {
			if (prev != null && prev.compareTo(val) > 0) {
				throw new IllegalArgumentException("Value " + prev + " precedes " + val + " in bucket edges list");
			}
			prev = val;
		}

		final int numBuckets = bucketEdges.size() -1;
		for (int i = 0; i < numBuckets; ++i) {
			T bot = bucketEdges.get(i);
			T top = bucketEdges.get(i + 1);
			if (typeOps.isDiscreteValued()) {
				double size = typeOps.getBucketSize(bot, top);
				if (size < distinctVals[i]) {
					if ((size * 1.01) > distinctVals[i]) {
						distinctVals[i] = size;
					} else {
						throw new IllegalArgumentException("Bucket [" + bot + "," + top + ") claims to contain " + distinctVals[i] + " distinct values, but there are only " + size + " distinct values in that range");
					}
				}
			}
			if (cardinality[i] < distinctVals[i]) {
				cardinality[i] = distinctVals[i];
//				throw new IllegalArgumentException("Bucket [" + bot + "," + top + ") has cardinality (" + cardinality[i] + ") < distinct values (" + distinctVals[i] + ")");
			}
			if (cardinality[i] == 0.0 && distinctVals[i] != 0.0) {
				throw new IllegalArgumentException("Bucket [" + bot + "," + top + ") has cardinality (" + cardinality[i] + ") = 0 but distinct values (" + distinctVals[i] + ") != 0");
			}
			if (cardinality[i] != 0.0 && distinctVals[i] == 0.0) {
				throw new IllegalArgumentException("Bucket [" + bot + "," + top + ") has cardinality (" + cardinality[i] + ") != 0 but distinct values (" + distinctVals[i] + ") = 0");
			}
			if (cardinality[i] < 0) {
				throw new IllegalArgumentException("Bucket [" + bot + "," + top + ") has cardinality (" + cardinality[i] + ") < 0");
			}
			if (distinctVals[i] < 0) {
				throw new IllegalArgumentException("Bucket [" + bot + "," + top + ") has distinct values (" + distinctVals[i] + ") < 0");
			}
		}
	}

	public static Histogram<Double> createDoubleHistogram(List<Double> bucketEdges, double[] cardinality, double[] distinctVals) {
		return new DoubleHistogram(bucketEdges, cardinality, distinctVals, toDouble);
	}

	public static Histogram<Date> createDateHistogram(List<Date> bucketEdges, double[] cardinality, double[] distinctVals) {
		return new GenericHistogram<Date>(bucketEdges, cardinality, distinctVals, toDate);
	}

	public static Histogram<Integer> createIntegerHistogram(List<Integer> bucketEdges, double[] cardinality, double[] distinctVals) {
		return new IntHistogram(bucketEdges, cardinality, distinctVals, toInteger);
	}

	public static Histogram<String> createStringHistogram(int length, List<String> bucketEdges, double[] cardinality, double[] distinctVals) {
		for (String s : bucketEdges) {
			if (! s.equals(convertForHistogram(length, s))) {
				throw new IllegalArgumentException("All strings must be converted for histogram");
			}
		}

		return new GenericHistogram<String>(bucketEdges, cardinality, distinctVals, new TypeOpsString(length));
	}


	public interface TypeOps<T> extends Serializable {
		double getBucketSize(T bot, T top);
		T getPredecessor(T val);
		T getSuccessor(T val);
		boolean isDiscreteValued();
		T prepareForHistogram(T val);
		double getBucketMidpoint(T bot, T top);
		T fromNumber(Number n);
		T combine (T t1, T t2);
	}

	private static final TypeOps<Integer> toInteger = new TypeOps<Integer>() {
		private static final long serialVersionUID = 1L;

		public double getBucketSize(Integer bot, Integer top) {
			return top - bot;
		}

		public Integer getPredecessor(Integer val) {
			return val - 1;
		}


		public Integer getSuccessor(Integer val) {
			return val + 1;
		}

		public boolean isDiscreteValued() {
			return true;
		}

		public Integer prepareForHistogram(Integer val) {
			return val;
		}

		public double getBucketMidpoint(Integer bot, Integer top) {
			return bot + (top - bot) / 2.0;
		}

		public Integer fromNumber(Number n) {
			return n.intValue();
		}

		public Integer combine(Integer t1, Integer t2) {
			return t1 + t2;
		}
	};

	private static final TypeOps<Double> toDouble = new TypeOps<Double>() {
		private static final long serialVersionUID = 1L;

		public double getBucketSize(Double bot, Double top) {
			return top - bot;
		}

		public Double getPredecessor(Double val) {
			return val;
		}

		public Double getSuccessor(Double val) {
			return val;
		}

		public boolean isDiscreteValued() {
			return false;
		}

		public Double prepareForHistogram(Double val) {
			return val;
		}

		public double getBucketMidpoint(Double bot, Double top) {
			return bot + (top - bot) / 2;
		}

		public Double fromNumber(Number n) {
			return n.doubleValue();
		}

		public Double combine(Double t1, Double t2) {
			return t1 + t2;
		}

	};

	private static final TypeOps<Date> toDate = new TypeOps<Date>() {
		private static final long serialVersionUID = 1L;
		public double getBucketSize(Date bot, Date top) {

			// Deal with partial years
			if (top.getYear() == bot.getYear()) {
				return top.getDayOfYear() - bot.getDayOfYear();
			}
			// interval is [bot, top)
			int size;
			if (Date.isLeapYear(bot.getYear())) {
				size = 366 - bot.getDayOfYear();
			} else {
				size = 365 - bot.getDayOfYear();
			}
			size += top.getDayOfYear();
			
			// Deal with whole years
			for (int year = bot.getYear() + 1; year < top.getYear(); ++year) {
				if (Date.isLeapYear(year)) {
					size += 366;
				} else {
					size += 365;
				}
			}
			return size;
		}

		public Date getPredecessor(Date val) {
			return val.yesterday();
		}

		public Date getSuccessor(Date val) {
			return val.tomorrow();
		}

		public boolean isDiscreteValued() {
			return true;
		}

		public Date prepareForHistogram(Date val) {
			return val;
		}

		public double getBucketMidpoint(Date bot, Date top) {
			throw new UnsupportedOperationException();
		}

		public Date fromNumber(Number n) {
			throw new UnsupportedOperationException();
		}

		public Date combine(Date t1, Date t2) {
			throw new UnsupportedOperationException();
		}
	};

	private static char[] order = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
		'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U',
		'V', 'W', 'X', 'Y', 'Z', '0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', ' '};

	static {
		Arrays.sort(order);
	}

	private static long getStringNum(String s) {
		long result = 0;
		long mult = 1;
		for (int i = s.length() - 1; i >= 0; --i) {			
			int pos = Arrays.binarySearch(order, s.charAt(i));
			result += mult * pos;
			mult *= order.length;
		}
		return result;
	}

	private static String getStringFromNum(int length, long num) {
		char[] buf = new char[length];
		long mult = 1;
		for (int i = length - 1; i >= 0; --i) {
			long pos = (num / mult) % order.length;
			buf[i] = order[(int) pos];
			mult *= order.length;
		}
		return new String(buf);
	}

	private static class TypeOpsString implements TypeOps<String> {
		private static final long serialVersionUID = 1L;
		final int length;

		TypeOpsString(int length) {
			this.length = length;
		}

		public double getBucketSize(String bot, String top) {
			return getStringNum(top) - getStringNum(bot);
		}

		public String getPredecessor(String val) {
			return getStringFromNum(length, getStringNum(val) - 1);
		}

		public String getSuccessor(String val) {
			return getStringFromNum(length, getStringNum(val) + 1);
		}

		public boolean isDiscreteValued() {
			return true;
		}

		public String prepareForHistogram(String val) {
			return convertForHistogram(length, val);
		}

		public double getBucketMidpoint(String bot, String top) {
			throw new UnsupportedOperationException();
		}

		public String fromNumber(Number n) {
			throw new UnsupportedOperationException();
		}

		public String combine(String t1, String t2) {
			throw new UnsupportedOperationException();
		}

	}

	public static String convertForHistogram(int length, String input) {
		// This means that there are 37 possibilities for each position
		// (A-Z, 0-9, and space)
		StringBuilder sb = new StringBuilder();

		final int inputLength = input.length();

		for (int i = 0; i < length; ++i) {
			if (i >= inputLength) {
				sb.append(' ');
			} else {
				char c = input.charAt(i);
				c = Character.toUpperCase(c);
				if (Arrays.binarySearch(order, c) >= 0) {
					sb.append(c);
				} else {
					sb.append(' ');
				}
			}
		}

		return sb.toString();
	}

	public static String getSuccessorForHistogram(int length, String input) {
		String converted = convertForHistogram(length, input);
		long num = getStringNum(converted);
		++num;
		String succ = getStringFromNum(length, num);
		if (succ.compareTo(converted) < 0) {
			// Avoid ZZZ wrapping around to '   '
			return converted;
		} else {
			return succ;
		}
	}

	public static String getPredecessorForHistogram(int length, String input) {
		String converted = convertForHistogram(length, input);
		long num = getStringNum(converted);
		--num;
		return getStringFromNum(length, num);
	}


	private abstract class BucketCombinator {
		abstract Results combineResults(Results r1, Results r2, T bot, T top);
	}

	private Histogram<T> combine(Histogram<T> h1, Histogram<T> h2,
			BucketCombinator bc) {

		List<T> bucketEdges = combineSortedLists(h1.getBucketEdges(), h2.getBucketEdges());
		final int numBuckets = bucketEdges.size() - 1;
		double[] cards = new double[numBuckets], distinct = new double[numBuckets];

		for (int i = 0; i < numBuckets; ++i) {
			T low = bucketEdges.get(i);
			T nextBot = bucketEdges.get(i + 1);
			T high = typeOps.getPredecessor(nextBot);
			Results r1 = h1.getNumInRange(low, high);
			Results r2 = h2.getNumInRange(low, high);
			Results combined = bc.combineResults(r1, r2, low, nextBot);
			cards[i] = combined.cardinality;
			distinct[i] = combined.distinctValues;
		}

		return h1.createNewHistogram(bucketEdges, cards, distinct);
	}

	public final Histogram<T> union(Histogram<T> other) {
		if (isEmpty()) {
			return other;
		} else if (other.isEmpty()) {
			return this;
		}

		return combine(this, other, new BucketCombinator() {
			Results combineResults(Results r1, Results r2, T bot, T top) {
				double card = r1.cardinality + r2.cardinality;
				double distinct;
				if (typeOps.isDiscreteValued()) {
					double numVals = typeOps.getBucketSize(bot, top);
					double bigDV, smallDV;
					if (r1.distinctValues > r2.distinctValues) {
						bigDV = r1.distinctValues;
						smallDV = r2.distinctValues;
					} else {
						bigDV = r2.distinctValues;
						smallDV = r1.distinctValues;
					}
					distinct = bigDV + smallDV * (numVals - bigDV) / numVals;
				} else {
					distinct = r1.distinctValues + r2.distinctValues;
				}
				return new Results(card, distinct);
			}

		});
	}

	public final Histogram<T> semiJoinWith(Histogram<T> other) {
		return joinWith(other,true);
	}
	
	public final Histogram<T> joinWith(Histogram<T> other) {
		return joinWith(other,false);
	}
	
	public final Histogram<T> joinWith(Histogram<T> other, final boolean semi) {
		if (this.isEmpty() || other.isEmpty()) {
			List<T> empty = Collections.emptyList();
			double[] empty2 = new double[0];
			return createNewHistogram(empty, empty2, empty2);
		}

		return combine(this, other, new BucketCombinator() {
			Results combineResults(Results r1, Results r2, T bot, T top) {
				if (r1.cardinality == 0.0 || r2.cardinality == 0.0) {
					return empty;
				}
				double card;
				double distinct;
				if (typeOps.isDiscreteValued()) {
					double numVals = (double) typeOps.getBucketSize(bot, top);
					distinct = r1.distinctValues * r2.distinctValues / numVals;
					card = distinct * (r1.cardinality / r1.distinctValues);
					if (! semi) {
						card *= (r2.cardinality / r2.distinctValues);
					}
					if (distinct > numVals) {
						distinct = numVals;
					}
				} else {
					// There's really no good way to do this
					card = r1.cardinality / 10;
					distinct = (r1.distinctValues > r2.distinctValues ? r2.distinctValues : r1.distinctValues) / 10;
					if (! semi) {
						card *= r2.cardinality;
					}
				}
				return new Results(card, distinct);
			}
		});
	}

	/**
	 * Get a list containing the edges of the histogram buckets.
	 * Each bucket contains its lower bound but not its upper bound.
	 * 
	 * @return		The list of bucket edges
	 */
	public abstract List<T> getBucketEdges();

	protected abstract Histogram<T> createNewHistogram(List<T> bucketEdges, double[] cardinality, double[] distinctVals);


	private static <T extends Comparable<? super T>> List<T> combineSortedLists(List<T> l1, List<T> l2) {
		List<T> result = new ArrayList<T>(l1.size() + l2.size());

		int pos1 = 0, pos2 = 0;

		final int l1size = l1.size(), l2size = l2.size();

		while (pos1 < l1size || pos2 < l2size) {
			if (pos1 >= l1size) {
				result.add(l2.get(pos2++));
			} else if (pos2 >= l2size) {
				result.add(l1.get(pos1++));
			} else {
				T t1 = l1.get(pos1);
				T t2 = l2.get(pos2);
				if (t1.equals(t2)) {
					result.add(t1);
					++pos1;
					++pos2;
				} else if (t1.compareTo(t2) < 0) {
					result.add(t1);
					++pos1;
				} else {
					result.add(t2);
					++pos2;
				}
			}
		}

		return result;
	}

	public abstract boolean isEmpty();

	public static class Results {
		final double cardinality;
		final double distinctValues;

		public Results(double cardinality, double distinctValues) {
			this.cardinality = cardinality;
			this.distinctValues = distinctValues;
		}

		public String toString() {
			return "Cardinality: " + cardinality + " Distinct values: " + distinctValues;
		}
		
		public boolean equals(Object o) {
			if (o == null || o.getClass() != this.getClass()) {
				return false;
			}
			Results r = (Results) o;
			return r.cardinality == cardinality && r.distinctValues == distinctValues;
		}
	}

	static final Results empty = new Results(0,0);

	public Results getSize() {
		return getNumInRange(null,null);
	}
	
	/**
	 * Get the number of entries within the specified range (inclusive)
	 * 
	 * @param low			The lower bound, or <code>null</code> to omit
	 * @param high			The upper bound, or <code>null</code> to omit
	 * @return		The number of entries
	 */
	public Results getNumInRange(T low, T high) {
		if (cardinality.length == 0) {
			return empty;
		}

		if (low != null && high != null && low.compareTo(high) > 0) {
			throw new IllegalArgumentException("low must be <= high");
		}

		List<T> bucketEdges = getBucketEdges();

		
		if (low == null) {
			low = bucketEdges.get(0);
		} else {
			low = typeOps.prepareForHistogram(low);
		}
		if (high == null) {
			high = bucketEdges.get(bucketEdges.size() - 1);
		} else {
			high = typeOps.prepareForHistogram(high);
		}

		if ((high.compareTo(bucketEdges.get(0)) < 0) || (low.compareTo(bucketEdges.get(bucketEdges.size() - 1)) >= 0)) {
			return empty;
		}

		int index = Collections.binarySearch(bucketEdges, low);

		if (index < 0) {
			index = -index - 2;
		}
		if (index < 0) {
			index = 0;
		}

		double cardCount = 0;
		double distinctVals = 0;

		final int last = bucketEdges.size() -1;

		while (bucketEdges.get(index).compareTo(high) <= 0) {
			if (index == last) {
				break;
			}
			final T bottom = bucketEdges.get(index);
			final T top = typeOps.getPredecessor(bucketEdges.get(index + 1));
			final double interestedFrac;
			if (bottom.equals(top)) {
				interestedFrac = 1.0;
			} else {
				final T interestedBot = low.compareTo(bottom) < 0 ? bottom : low;
				final T interestedTop = high.compareTo(top) > 0 ? top : high;
				interestedFrac = ((double) typeOps.getBucketSize(interestedBot, typeOps.getSuccessor(interestedTop))) / typeOps.getBucketSize(bottom, typeOps.getSuccessor(top));
			}
			cardCount += cardinality[index] * interestedFrac;
			distinctVals += this.distinctVals[index] * interestedFrac;
			++index;
		}

		return new Results(cardCount, distinctVals);
	}

	public Histogram<T> scaleHistogram(double cardMultiplier, double distinctValsMultiplier) {
		final int length = cardinality.length;
		double[] newCards = new double[length], newDistinctVals = new double[length];

		for (int i = 0; i < length; ++i) {
			newCards[i] = cardinality[i] * cardMultiplier;
			newDistinctVals[i] = distinctVals[i] * distinctValsMultiplier;
		}

		return createNewHistogram(getBucketEdges(), newCards, newDistinctVals);
	}

	/**
	 * Create a new histogram excluding the specified range
	 * (inclusive)
	 * 
	 * @param lower		The lower bound, or <code>null</code> to use the
	 * 					lower bound of the histogram
	 * @param upper		The upper bound, or <code>null</code> to use the
	 * 					upper bound of the histogram
	 * @return			The new histogram
	 */
	public Histogram<T> removeRange(T lower, T upper) {
		List<T> bucketEdges = getBucketEdges();
		
		if (lower != null && upper != null && lower.compareTo(upper) > 0) {
			throw new IllegalArgumentException("lower must be <= upper");
		}
		if (lower == null) {
			lower = bucketEdges.get(0);
		} else {
			lower = typeOps.prepareForHistogram(lower);
		}
		if (upper == null) {
			upper = bucketEdges.get(bucketEdges.size() - 1);
		} else {
			upper = typeOps.prepareForHistogram(upper);
		}

		if ((upper.compareTo(bucketEdges.get(0)) < 0) || (lower.compareTo(bucketEdges.get(bucketEdges.size() - 1)) >= 0)) {
			return this;
		}

		
		List<Double> newCards = new ArrayList<Double>(cardinality.length);
		List<Double> newDVs = new ArrayList<Double>(cardinality.length);
		List<T> newEdges = new ArrayList<T>(bucketEdges.size());

		final int last = bucketEdges.size() - 1;

		for (int i = 0; i < last; ++i) {
			T buckBot = bucketEdges.get(i), buckTop = typeOps.getPredecessor(bucketEdges.get(i + 1)),
			nextBuckBot = bucketEdges.get(i + 1);
			if ((buckTop.compareTo(lower) < 0) || (buckBot.compareTo(upper) > 0)) {
				// Bucket is disjoint from range
				if (newEdges.isEmpty()) {
					newEdges.add(buckBot);
				}
				newEdges.add(nextBuckBot);
				newCards.add(cardinality[i]);
				newDVs.add(distinctVals[i]);
			} else if (buckBot.compareTo(lower) <= 0 && buckTop.compareTo(upper) >= 0) {
				// Bucket contains entire range
				if (! buckBot.equals(lower)) {
					if (newEdges.isEmpty()) {
						newEdges.add(buckBot);
					}
					T pred = typeOps.getPredecessor(lower);
					newEdges.add(pred);
					Results r = getNumInRange(buckBot,pred);
					newCards.add(r.cardinality);
					newDVs.add(r.distinctValues);
				}
				T succ = typeOps.getSuccessor(upper);
				if (! newEdges.isEmpty()) {
					newEdges.add(succ);
					newCards.add(0.0);
					newDVs.add(0.0);
				}
				if (! buckTop.equals(upper)) {
					if (newEdges.isEmpty()) {
						newEdges.add(succ);
					}
					newEdges.add(nextBuckBot);
					Results r = getNumInRange(succ, buckTop);
					newCards.add(r.cardinality);
					newDVs.add(r.distinctValues);
				}
			} else if(buckBot.compareTo(lower) >= 0 && buckTop.compareTo(upper) <= 0) {
				// Bucket is contained in range
			} else if (buckBot.compareTo(lower) <= 0 && buckTop.compareTo(lower) >= 0) {
				// Bucket contains lower end of range
				if (newEdges.isEmpty()) {
					newEdges.add(buckBot);
				}

				T pred = typeOps.getPredecessor(lower);
				newEdges.add(lower);
				Results r = getNumInRange(buckBot, pred);
				newCards.add(r.cardinality);
				newDVs.add(r.distinctValues);
			} else if (buckBot.compareTo(upper) <= 0 && buckTop.compareTo(upper) >= 0) {
				// Bucket contains upper end of range
				if (newEdges.isEmpty()) {
					newEdges.add(typeOps.getSuccessor(upper));
				} else {
					newEdges.add(typeOps.getSuccessor(upper));
					newCards.add(0.0);
					newDVs.add(0.0);
				}

				newEdges.add(nextBuckBot);
				Results r = getNumInRange(typeOps.getSuccessor(upper), buckTop);
				newCards.add(r.cardinality);
				newDVs.add(r.distinctValues);
			} else {
				throw new RuntimeException("Shouldn't get here...");
			}
		}

		return createNewHistogram(newEdges, convertDoubleList(newCards), convertDoubleList(newDVs));
	}

	/**
	 * Create a new histogram containing only the specified range
	 * (inclusive)
	 * 
	 * @param lower		The lower bound, or <code>null</code> to use the
	 * 					lower bound of the histogram
	 * @param upper		The upper bound, or <code>null</code> to use the
	 * 					upper bound of the histogram
	 * @return			The new histogram
	 */
	public Histogram<T> retainRange(T lower, T upper) {
		List<T> bucketEdges = getBucketEdges();

		if (lower != null && upper != null && lower.compareTo(upper) > 0) {
			throw new IllegalArgumentException("lower must be <= upper");
		}
		
		if (lower == null) {
			lower = bucketEdges.get(0);
		} else {
			lower = typeOps.prepareForHistogram(lower);
		}
		if (upper == null) {
			upper = bucketEdges.get(bucketEdges.size() - 1);
		} else {
			upper = typeOps.prepareForHistogram(upper);
		}

		if ((upper.compareTo(bucketEdges.get(0)) < 0) || (lower.compareTo(bucketEdges.get(bucketEdges.size() - 1)) >= 0)) {
			return this.empty();
		}

		
		List<Double> newCards = new ArrayList<Double>(cardinality.length);
		List<Double> newDVs = new ArrayList<Double>(cardinality.length);
		List<T> newEdges = new ArrayList<T>(bucketEdges.size());

		final int last = bucketEdges.size() - 1;

		for (int i = 0; i < last; ++i) {
			T buckBot = bucketEdges.get(i), buckTop = typeOps.getPredecessor(bucketEdges.get(i + 1)),
			nextBuckBot = bucketEdges.get(i + 1);
			if (buckTop.compareTo(lower) < 0) {
				continue;
			} else if (buckBot.compareTo(upper) > 0) {
				break;
			} else if (buckBot.compareTo(lower) <= 0 && buckTop.compareTo(upper) >= 0) {
				// Entire range is within bucket
				newEdges.add(lower);
				newEdges.add(typeOps.getSuccessor(upper));
				Results r = getNumInRange(lower, upper);
				newCards.add(r.cardinality);
				newDVs.add(r.distinctValues);
				break;
			} else if (buckBot.compareTo(lower) >= 0 && buckTop.compareTo(upper) <= 0) {
				// Entire bucket is within range
				if (newEdges.isEmpty()) {
					newEdges.add(buckBot);
				}
				newEdges.add(nextBuckBot);
				newCards.add(cardinality[i]);
				newDVs.add(distinctVals[i]);
			} else if (buckBot.compareTo(lower) <= 0 && buckTop.compareTo(lower) >= 0) {
				// Bucket contains lower end of range
				if (newEdges.isEmpty()) {
					newEdges.add(lower);
				}
				newEdges.add(nextBuckBot);
				Results r = getNumInRange(lower, buckTop);
				newCards.add(r.cardinality);
				newDVs.add(r.distinctValues);
			} else if (buckBot.compareTo(upper) <= 0 && buckTop.compareTo(upper) >= 0) {
				// Bucket contains upper end of range
				if (newEdges.isEmpty()) {
					throw new RuntimeException("Shouldn't happen...");
				}
				newEdges.add(typeOps.getSuccessor(upper));
				Results r = getNumInRange(buckBot, upper);
				newCards.add(r.cardinality);
				newDVs.add(r.distinctValues);
			} else {
				throw new RuntimeException("Shouldn't get here...");
			}
		}

		return createNewHistogram(newEdges, convertDoubleList(newCards), convertDoubleList(newDVs));
	}	

	/**
	 * Create a new histogram with all duplicates eliminated
	 * 
	 * @return		The new histogram
	 */
	public Histogram<T> removeDuplicates() {
		return createNewHistogram(this.getBucketEdges(), this.distinctVals, this.distinctVals);
	}

	public Histogram<T> createAggregateHistogram(AggFunc aggFunc, double numBuckets) {
		if (this.isEmpty()) {
			return this;
		}
		// TODO: do something more sophisticated here
		if (aggFunc == AggFunc.MAX || aggFunc == AggFunc.MIN || aggFunc == AggFunc.AVG) {
			List<T> edges = getBucketEdges();
			T min = edges.get(0), max = edges.get(edges.size() - 1);
			List<T> newEdges = new ArrayList<T>(2);
			newEdges.add(min);
			newEdges.add(max);
			double DVs = 0.0;
			if (typeOps.isDiscreteValued()) {
				double buckSize = typeOps.getBucketSize(min, max);
				for (int j = 0; j < numBuckets; ++j) {
					double frac = numBuckets - j;
					if (frac > 1.0) {
						frac = 1.0;
					}
					DVs += frac * (buckSize - DVs) / buckSize;
				}
			} else {
				DVs = numBuckets;
			}
			return createNewHistogram(newEdges, new double[] {numBuckets}, new double[]{DVs});
		} else if (aggFunc == AggFunc.SUM) {
			List<T> edges = getBucketEdges();
			T min = edges.get(0);
			List<T> newEdges = new ArrayList<T>(2);
			newEdges.add(min);
			// Assume that no bucket will get more than four times
			// an even share of the data
			double remaining = 4 * getNumInRange(null,null).cardinality / numBuckets;
			double max = 0.0;
			for (int i = edges.size() - 2; i >= 0; --i) {
				double mult;
				if (cardinality[i] > remaining) {
					mult = cardinality[i];
				} else {
					mult = remaining;
				}
				max += mult * typeOps.getBucketMidpoint(edges.get(i), edges.get(i+1));
				remaining -= mult;
				if (mult <= 0.0) {
					break;
				}
			}
			T maxT = typeOps.fromNumber(max);
			newEdges.add(maxT);
			double DVs = 0.0;
			if (typeOps.isDiscreteValued()) {
				double buckSize = typeOps.getBucketSize(min, maxT);
				for (int j = 0; j < numBuckets; ++j) {
					double frac = numBuckets - j;
					if (frac > 1.0) {
						frac = 1.0;
					}
					DVs += frac * (buckSize - DVs) / buckSize;
				}
			} else {
				DVs = numBuckets;
			}

			return createNewHistogram(newEdges, new double[]{numBuckets}, new double[] {DVs});	
		} else {
			throw new IllegalArgumentException("Don't know how to process aggregate function" + aggFunc);
		}
	}

	public Histogram<T> shift(Number amount) {
		List<T> bucketEdges = getBucketEdges();
		List<T> newBucketEdges = new ArrayList<T>(bucketEdges.size());

		T shift = typeOps.fromNumber(amount);

		for (T val : bucketEdges) {
			newBucketEdges.add(typeOps.combine(val, shift));
		}

		return createNewHistogram(newBucketEdges, this.cardinality, this.distinctVals);
	}

	/**
	 * Generate a histogram containing the results of counting the
	 * number of values in this histogram
	 * 
	 * @param count				The number of values overall
	 * @param numBuckets		The number of buckets the elements
	 * 							from this histogram will be grouped
	 * 							into
	 * @return					The derived histogram
	 */
	public static Histogram<Integer> createCountHistogram(double count, double numBuckets) {
		double mean = count / numBuckets;

		// Assume the number of buckets with a particular count is
		// normally distributed with mean count/numBuckets and
		// standard deviation mean / 10

		List<Integer> bucketEdges = new ArrayList<Integer>();

		bucketEdges.add(0);
		int last = 0;
		int intCount = (int) Math.round(count);
		for (double mult = 0.7; mult < 1.35; mult += 0.1) {
			int next = (int) Math.round(mult * mean);
			if (next >= intCount) {
				break;
			}
			if (next != last) {
				bucketEdges.add(next);
				last = next;
			}
		}
		if (last != intCount) {
			bucketEdges.add(intCount);
		}

		final int numDerivedBuckets = bucketEdges.size() - 1;

		double cards[] = new double[numDerivedBuckets];
		double dvs[] = new double[numDerivedBuckets];
		for (int i = 0; i < numDerivedBuckets; ++i) {
			double botZ, topZ;
			if (i == 0) {
				botZ = Double.NEGATIVE_INFINITY;
			} else {
				botZ = (bucketEdges.get(i) - mean) / (mean / 10);
			}
			if (i == (numDerivedBuckets - 1)) {
				topZ = Double.POSITIVE_INFINITY;
			} else {
				topZ = (bucketEdges.get(i + 1) - mean) / (mean / 10);				
			}

			double num = numBuckets * (Phi(topZ) - Phi(botZ));
			cards[i] = num;
			int buckSize = bucketEdges.get(i + 1) - bucketEdges.get(i);
			int intNum = (int) Math.floor(num);
			double DVs = 0.0;
			for (int j = 0; j < intNum; ++j) {
				DVs += (buckSize - DVs) / buckSize;
			}
			DVs += (num - intNum) * (buckSize - DVs) / buckSize; 
			dvs[i] = DVs;
		}

		return createIntegerHistogram(bucketEdges, cards, dvs);
	}

	public static double[] convertDoubleList(List<Double> list) {
		double[] retval = new double[list.size()];
		int pos = 0;
		Iterator<Double> it = list.iterator();
		while (it.hasNext()) {
			retval[pos++] = it.next();
		}
		return retval;
	}

	/*
	 * These statistical functions were taken from
	 * http://www.cs.princeton.edu/introcs/21function/Gaussian.java
	 * 
	 */
	// return Phi(z) = standard Gaussian cdf using Taylor approximation
	private static double Phi(double z) {
		if (z < -8.0) return 0.0;
		if (z >  8.0) return 1.0;
		double sum = 0.0, term = z;
		for (int i = 3; sum + term != sum; i += 2) {
			sum  = sum + term;
			term = term * z * z / i;
		}
		return 0.5 + sum * phi(z);
	}

	// return phi(x) = standard Gaussian pdf
	private static double phi(double x) {
		return Math.exp(-x*x / 2) / Math.sqrt(2 * Math.PI);
	}

	public T getMinValue() {
		return getBucketEdges().get(0);
	}

	public T getMaxValue() {
		List<T> edges = getBucketEdges();
		return typeOps.getPredecessor(edges.get(edges.size() - 1));
	}
	
	double[] getCards() {
		double[] retval = new double[cardinality.length];
		System.arraycopy(cardinality, 0, retval, 0, cardinality.length);
		return retval;
	}
	
	double[] getDVs() {
		double[] retval = new double[distinctVals.length];
		System.arraycopy(distinctVals, 0, retval, 0, cardinality.length);
		return retval;
	}
	
	public double getNumDistinctValues() {
		double retval = 0.0;
		for (double d : distinctVals) {
			retval += d;
		}
		return retval;
	}
	
	public double getCard() {
		double retval = 0.0;
		for (double d : cardinality) {
			retval += d;
		}
		return retval;
	}
	
	private static final Histogram<?> emptyHist;
	
	static {
		double[] cards = new double[0], DVs = cards;
		List<Integer> edges = Collections.emptyList();
		emptyHist = new GenericHistogram<Integer>(edges, cards, DVs, toInteger);
	}
	
	@SuppressWarnings("unchecked")
	private Histogram<T> empty() {
		return (Histogram<T>) emptyHist;
	}
}
