package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.Type;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.NotNumericalException;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.optimization.Aggregate.AggFunc;

public class HashAggregator<M> extends Operator<M> {
	private final QpSchema inputSchema, outputSchema;
	private final OutputColumn[] outputColumns;
	private final int numAggs;
	private final AbstractRelation.FieldSource[] fss;
	private static final Logger logger = Logger.getLogger(HashAggregator.class);
	@SuppressWarnings("unused")
	private final QueryExecution<M> qe;

	private class GroupData {
		protected QpTuple<M> forGroupingCols;
		private final Aggregator[] aggregators = new Aggregator[numAggs];
		private int numTuples = 0;
		private final boolean needGroupingCols;

		GroupData() {
			this(true);
		}
		
		GroupData(boolean needGroupingCols) {
			this.needGroupingCols = needGroupingCols;
			int pos = 0;
			for (int i = 0; i < outputColumns.length; ++i) {
				OutputColumn oc = outputColumns[i];
				if (oc instanceof AggColumn) {
					Aggregator agg;
					Type t = oc.inputCol >= 0 ? inputSchema.getColType(oc.inputCol) : null;
					AggColumn ac = (AggColumn) oc;
					if (ac.func == AggFunc.AVG) {
						agg = new AvgAggregator(t, oc.inputCol);
					} else if (ac.func == AggFunc.COUNT) {
						agg = new CountAggregator(oc.inputCol);
					} else if (ac.func == AggFunc.MAX) {
						agg = new MaxAggregator(t, oc.inputCol);
					} else if (ac.func == AggFunc.MIN) {
						agg = new MinAggregator(t, oc.inputCol);
					} else if (ac.func == AggFunc.SUM) {
						agg = new SumAggregator(t, oc.inputCol);
					} else {
						throw new RuntimeException("Need to implement aggregate function " + ac);
					}
					aggregators[pos++] = agg;
				} else if (oc instanceof RewrittenAvgColumn) {
					RewrittenAvgColumn rac = (RewrittenAvgColumn) oc;
					Aggregator agg = new RewrittenAvgAggregator(inputSchema.getColType(rac.inputCol), rac.inputCol, rac.countCol);
					aggregators[pos++] = agg;
				}
			}
		}

		int getNumTuples() {
			return numTuples;
		}

		protected QpTuple<?> getGroupingCols() {
			return forGroupingCols;
		}		

		protected final synchronized void reset() {
			numTuples = 0;
			for (Aggregator agg : aggregators) {
				if (agg != null) {
					agg.reset();
				}
			}
		}

		final synchronized QpTuple<M> getResult(int phaseNo, M metadata) throws NotNumericalException, ValueMismatchException, CompareMismatch {
			Object[] aggFields = new Object[aggregators.length];
			for (int i = 0; i < aggregators.length; ++i) {
				aggFields[i] = aggregators[i].getResult();
			}
			AbstractRelation.RelationMapping rm = new AbstractRelation.RelationMapping(inputSchema, outputSchema, aggFields, fss);
			numTuplesProcessed += this.numTuples;
			return new QpTuple<M>(outputSchema, getGroupingCols(), rm, (InetSocketAddress[]) null, metadata, mdf, phaseNo);
		}

		final synchronized QpTuple<M> getResult(int phaseNo, M metadata, Set<InetSocketAddress> nodeIds) throws NotNumericalException, ValueMismatchException, CompareMismatch {
			Object[] aggFields = new Object[aggregators.length];
			for (int i = 0; i < aggregators.length; ++i) {
				aggFields[i] = aggregators[i].getResult();
			}
			AbstractRelation.RelationMapping rm = new AbstractRelation.RelationMapping(inputSchema, outputSchema, aggFields, fss);
			numTuplesProcessed += this.numTuples;
			return new QpTuple<M>(outputSchema, getGroupingCols(), rm, nodeIds, metadata, mdf, phaseNo);
		}

		synchronized QpTuple<M> getResult(int phaseNo, int lastPhaseToInclude) throws NotNumericalException, ValueMismatchException, CompareMismatch {
			return getResult(phaseNo, null);
		}

		private synchronized void addTuple(QpTuple<M> t, int count) throws CompareMismatch, NotNumericalException, ValueMismatchException {
			if (forGroupingCols == null && needGroupingCols) {
				forGroupingCols = t.createSelfContainedCopy();
			}
			for (Aggregator agg : aggregators) {
				if (agg != null) {
					agg.addTuple(t, count);
				}
			}
			++numTuples;
		}

		synchronized void addTuple(QpTuple<M> t) throws CompareMismatch, NotNumericalException, ValueMismatchException {
			addTuple(t, 1);
		}

		synchronized void addFromGroup(GroupData gd) throws CompareMismatch, NotNumericalException, ValueMismatchException {
			if (this.forGroupingCols == null && gd.forGroupingCols != null) {
				this.forGroupingCols = gd.forGroupingCols;
			}
			numTuples += gd.numTuples;
			for (int i = 0; i < aggregators.length; ++i) {
				aggregators[i].mergeDataFrom(gd.aggregators[i]);
			}
		}

		int disposeOfFailedNodeTuples(InetSocketAddress[] failedNodes) {
			throw new UnsupportedOperationException();
		}
	}

	private static class InetSocketAddressSet {
		private final InetSocketAddress[] nodes;
		private final int hashCode;
		final int phaseNo;

		InetSocketAddressSet(QpTuple<?> t) {
			nodes = t.getContributingNodes();
			phaseNo = t.getPhase();
			int hashCode = 37 * phaseNo;
			for (InetSocketAddress isa : nodes) {
				hashCode += isa.hashCode();
			}
			this.hashCode = hashCode;
		}

		public int hashCode() {
			return hashCode;
		}

		boolean containsNode(InetSocketAddress node) {
			for (InetSocketAddress isa : nodes) {
				if (node.equals(isa)) {
					return true;
				}
			}
			return false;
		}

		public boolean equals(Object o) {
			InetSocketAddressSet isas = (InetSocketAddressSet) o;
			if (isas.hashCode != hashCode || isas.nodes.length != nodes.length || phaseNo != isas.phaseNo) {
				return false;
			}

			// Make sure that all nodes from the one object are in the other.
			// If they are, since they have the same length and no duplicates,
			// the two sets must be equal
			final int numNodes = nodes.length;
			OUTER: for (int i = 0; i < numNodes; ++i) {
				for (int j = 0; j < numNodes; ++j) {
					if (nodes[i].equals(isas.nodes[j])) {
						continue OUTER;
					}
				}
				return false;
			}
			return true;
		}

		void addToCollection(Collection<InetSocketAddress> c) {
			for (InetSocketAddress isa : nodes) {
				c.add(isa);
			}
		}

		public String toString() {
			return "(" + Arrays.toString(nodes) + "," + phaseNo + ")";
		}
	}

	private class RecoverableGroupData extends GroupData {
		private final Map<InetSocketAddressSet,GroupData> data = new HashMap<InetSocketAddressSet,GroupData>();
		private int lastPhaseSent = -1;

		@Override
		synchronized void addTuple(QpTuple<M> t) throws CompareMismatch, NotNumericalException, ValueMismatchException {
			InetSocketAddressSet isas = new InetSocketAddressSet(t);
			GroupData gd = data.get(isas);
			if (gd == null) {
				gd = new GroupData(false);
				data.put(isas, gd);
			}
			gd.addTuple(t, 1);
			
			if (this.forGroupingCols == null) {
				forGroupingCols = t.createSelfContainedCopy();
			}
		}

		@Override
		synchronized int getNumTuples() {
			int count = 0;
			for (GroupData gd : data.values()) {
				count += gd.numTuples;
			}
			return count;
		}

		@Override
		synchronized QpTuple<M> getResult(int phaseNo, int lastPhaseToInclude) throws NotNumericalException, ValueMismatchException, CompareMismatch {
			if (data.isEmpty()) {
				return null;
			}
			reset();
			boolean send = false;
			Set<InetSocketAddress> nodeIds = new HashSet<InetSocketAddress>();
			for (Map.Entry<InetSocketAddressSet, GroupData> me : data.entrySet()) {
				final InetSocketAddressSet isas = me.getKey();
				if (isas.phaseNo > lastPhaseSent && isas.phaseNo <= lastPhaseToInclude) {
					this.addFromGroup(me.getValue());
					isas.addToCollection(nodeIds);
					send = true;
				} else {
					numOtherPhaseTuples += me.getValue().numTuples;
				}
			}
			if (send) {
				lastPhaseSent = phaseNo;
				return getResult(phaseNo, null, nodeIds);

			} else {
				return null;
			}
		}

		synchronized void resendTuples(int newPhaseNo, int oldPhaseNo, IdRangeSet failedRanges, int[] outputSchemaIdCols, QpTupleBag<M> output) throws CompareMismatch, NotNumericalException, ValueMismatchException {
			if (data.isEmpty() || oldPhaseNo > lastPhaseSent) {
				return;
			}
			reset();
			boolean resend = false;
			Set<InetSocketAddress> nodeIds = new HashSet<InetSocketAddress>();
			for (Map.Entry<InetSocketAddressSet, GroupData> me : data.entrySet()) {
				final InetSocketAddressSet isas = me.getKey();
				if (isas.phaseNo <= lastPhaseSent) {
					this.addFromGroup(me.getValue());
					isas.addToCollection(nodeIds);
					resend = true;
				} else {
					numOtherPhaseTuples += me.getValue().numTuples;
				}
			}
			if (resend) {
				QpTuple<M> result = getResult(newPhaseNo, null, nodeIds);
				Id id = result.getQPid(outputSchemaIdCols);
				if (failedRanges.contains(id)) {
					output.add(result);
				}
			}
		}

		@Override
		synchronized int disposeOfFailedNodeTuples(InetSocketAddress[] failedNodes) {
			Iterator<Map.Entry<InetSocketAddressSet,GroupData>> it = data.entrySet().iterator();

			int removedCount = 0;

			OUTER: while (it.hasNext()) {
				Map.Entry<InetSocketAddressSet, GroupData> me = it.next();
				final InetSocketAddressSet isas = me.getKey();
				for (InetSocketAddress failed : failedNodes) {
					if (isas.containsNode(failed)) {
						int count = me.getValue().getNumTuples();
						if (logger.isDebugEnabled()) {
							logger.debug("Dropping " + count + " tuples from bucket for " + getGroupingCols() + " for origin set " + isas);
						}
						removedCount += count;
						it.remove();
						lastPhaseSent = -1;
						continue OUTER;
					}
				}
			}

			return removedCount;
		}
	}

	private class GroupDataWithMetadata extends GroupData {
		private final Map<QpTuple<M>,M> metadataMap = new HashMap<QpTuple<M>,M>();

		@Override
		synchronized void addTuple(QpTuple<M> t) {
			M metadata = t.getMetadata(schemas, mdf);
			t = t.createSelfContainedCopy();
			if (this.metadataMap.containsKey(t)) {
				M newMetadata = mdf.addMetadata(metadata, this.metadataMap.get(t));
				if (mdf.isZero(newMetadata)) {
					this.metadataMap.remove(t);
				} else {
					this.metadataMap.put(t, newMetadata);
				}
			} else {
				this.metadataMap.put(t, metadata);
			}
		}

		@SuppressWarnings("unchecked")
		synchronized QpTuple<M> getResult(QpTuple<?> groupingCols, int phaseNo) throws NotNumericalException, ValueMismatchException, CompareMismatch {
			reset();
			for (Map.Entry<QpTuple<M>,M> me : metadataMap.entrySet()) {
				final QpTuple<?> t = me.getKey();
				final int card = mdf.getCardinality(me.getValue());
				super.addTuple((QpTuple<M>) t, card);
			}
			return super.getResult(phaseNo, mdf.agg(HashAggregator.this, metadataMap.values()));
		}

		int getNumTuples() {
			int card = 0;
			for (M m : metadataMap.values()) {
				card += mdf.getCardinality(m);
			}
			return card;
		}
	}

	private class RecoverableGroupDataWithMetadata extends GroupData {

		synchronized void addTuple(QpTuple<M> t) {
			throw new UnsupportedOperationException("Need to implement");
		}

		synchronized QpTuple<M> getResult(int phaseNo, int lastPhaseToInclude) throws NotNumericalException, ValueMismatchException, CompareMismatch {
			throw new UnsupportedOperationException("Need to implement");
		}

		@Override
		synchronized int disposeOfFailedNodeTuples(InetSocketAddress[] failedNodes) {
			throw new UnsupportedOperationException("Need to implement");
		}
	}


	private SynchronizingMap<byte[],GroupData> groups;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock readLock = lock.readLock();
	private final Lock writeLock = lock.writeLock();
	private final int[] outputSchemaIdCols;
	private final int[] groupingColumnsArray;

	public HashAggregator(Operator<M> dest, WhichInput outputDest,
			QpSchema inputSchema, QpSchema outputSchema,
			List<Integer> groupingColumns, List<OutputColumn> outputCols, InetSocketAddress nodeId, int operatorId,
			RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, Map<Integer,int[]> recoveryOperators, QueryExecution<M> qe) {
		super(dest,outputDest,nodeId,operatorId,mdf,schemas,rt,recoveryOperators != null);
		this.qe = qe;
		this.inputSchema = inputSchema;
		this.outputSchema = outputSchema;
		groupingColumnsArray = new int[groupingColumns.size()];
		int pos = 0;
		for (int col : groupingColumns) {
			groupingColumnsArray[pos] = col;
			++pos;
		}

		if (enableRecovery) {
			int[] outputSchemaIdCols = recoveryOperators.get(operatorId);
			if (outputSchemaIdCols == null) {
				this.outputSchemaIdCols = null;
			} else {
				this.outputSchemaIdCols = new int[outputSchemaIdCols.length];
				for (int i = 0; i < outputSchemaIdCols.length; ++i) {
					this.outputSchemaIdCols[i] = outputSchemaIdCols[i];
				}
			}
		} else {
			outputSchemaIdCols = null;
		}

		outputColumns = new OutputColumn[outputSchema.getNumCols()];
		groups = new SynchronizingMap<byte[],GroupData>(new SynchronizingMap.SupportingOps<byte[],GroupData>() {
			public GroupData getNewValue() {
				if (HashAggregator.this.mdf == null) {
					if (HashAggregator.this.enableRecovery) {
						return new RecoverableGroupData();
					} else {
						return new GroupData();
					}
				} else {
					if (HashAggregator.this.enableRecovery) {
						return new RecoverableGroupDataWithMetadata();
					} else {
						return new GroupDataWithMetadata();
					}
				}
			}

			@Override
			public boolean equals(byte[] key1, byte[] key2) {
				return Arrays.equals(key1, key2);
			}

			@Override
			public int hash(byte[] key) {
				return Arrays.hashCode(key);
			}
		});

		if (outputCols.size() != outputColumns.length) {
			throw new IllegalArgumentException("Need " + outputColumns.length + " output columns, but got " + outputCols.size());
		}

		Set<Integer> retainColsSet = new HashSet<Integer>();
		pos = 0;
		int numAggs = 0;
		final int inputSize = inputSchema.getNumCols();
		fss = new AbstractRelation.FieldSource[outputColumns.length];
		for (OutputColumn col  : outputCols) {
			if (col instanceof AggColumn) {
				fss[pos] = new AbstractRelation.FieldSource(numAggs,false);
			} else if (col instanceof RewrittenAvgColumn) {
				fss[pos] = new AbstractRelation.FieldSource(numAggs,false);
			} else {
				fss[pos] = new AbstractRelation.FieldSource(col.inputCol,true);
			}

			Collection<Integer> inputs = col.getInputs();
			retainColsSet.addAll(inputs);
			for (Integer input : inputs) {
				if (input < 0 || input > inputSize) {
					throw new IllegalArgumentException("Input column out of range: " + input);
				}
			}
			if (col.isAggregate()) {
				for (Integer input : inputs) {
					if (groupingColumns.contains(input)) {
						throw new IllegalArgumentException("Input to aggregate must not be a grouping column: " + input);
					}
				}
				++numAggs;
			} else {
				for (Integer input : inputs) {
					if (! groupingColumns.contains(input)) {
						throw new IllegalArgumentException("Output column must be a grouping column: " + input);
					}
				}
			}
			outputColumns[pos++] = col;
		}

		this.numAggs = numAggs;
	}

	public void receiveTuples(WhichInput input, QpTupleBag<M> tuples) {
		if (tuples.isEmpty()) {
			return;
		}

		int numTuplesReceived = 0;
		int numTuplesDisposed = 0;

		readLock.lock();

		try {
			Iterator<QpTuple<M>> it = tuples.recyclingIterator();
			TUPLE: while (it.hasNext()) {
				QpTuple<M> t = it.next();
				++numTuplesReceived;
				if (t.getPhase() <= this.lastPhaseFinished) {
					reportException(new RuntimeException("Finished through phase " + this.lastPhaseFinished + " but recived tuple from phase " + t.getPhase() + ": " + t));
					return;
				}
				if (this.enableRecovery && this.failedNodes != null) {
					for (InetSocketAddress node : failedNodes) {
						if (t.contributes(node)) {
							++numTuplesDisposed;
							if (logger.isDebugEnabled()) {
								logger.debug("Dropping tuple " + t + " with origin set " + t.getContributingNodesSet());
							}
							continue TUPLE;
						}
					}
				}
				groups.getOrCreate(t.getUniqueBytesForColumns(groupingColumnsArray)).addTuple(t);
			}
		} catch (Exception e) {
			reportException(e);
		} finally {
			readLock.unlock();
		}

		if (logger.isDebugEnabled()) {
			synchronized (this) {
				this.numTuplesReceived += numTuplesReceived;
				this.numTuplesDisposed += numTuplesDisposed;
			}
		}
	}

	void reset () {
		groups.clear();
	}

	public static class OutputColumn {
		final int inputCol;
		public OutputColumn(int inputCol) {
			this.inputCol = inputCol;
		}

		boolean isAggregate() {
			return false;
		}
		Collection<Integer> getInputs() {
			if (inputCol >= 0) {
				return Collections.singleton(inputCol);
			} else {
				return Collections.emptySet();
			}
		}
	}

	public static class AggColumn extends OutputColumn {
		final AggFunc func;
		public AggColumn(int inputCol, AggFunc func) {
			super(inputCol);
			this.func = func;
		}
		public AggColumn(AggFunc func) {
			super(-1);
			if (! func.canBeNullary) {
				throw new IllegalArgumentException("Aggregate function " + func + " cannot be nullary");
			}
			this.func = func;
		}
		boolean isAggregate() {
			return true;
		}
	}

	public static class RewrittenAvgColumn extends OutputColumn {
		final int countCol;

		public RewrittenAvgColumn(int sumCol, int countCol) {
			super(sumCol);
			this.countCol = countCol;
		}
		boolean isAggregate() {
			return true;
		}
		Collection<Integer> getInputs() {
			return Arrays.asList(inputCol,countCol);
		}
	}

	protected void close() {
		groups = null;
	}

	private interface Aggregator {
		void addTuple(QpTuple<?> t, int count) throws NotNumericalException, ValueMismatchException, CompareMismatch;
		Object getResult() throws NotNumericalException, ValueMismatchException;
		void reset();
		void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException, CompareMismatch, NotNumericalException, ValueMismatchException;
	}

	private static class AvgAggregator implements Aggregator {
		private final Type t;
		private final int inputCol;
		private Object sum;
		private int count;

		AvgAggregator(Type t, int inputCol) {
			this.t = t;
			this.inputCol = inputCol;
			sum = null;
		}

		@Override
		public void addTuple(QpTuple<?> t, int count) throws NotNumericalException, ValueMismatchException {
			Object o = t.get(inputCol);
			if (o != null) {
				if (count == 1) {
					sum = this.t.add(o, sum);					
				} else {
					sum = this.t.add(this.t.multiply(o, count), sum);
				}
				this.count += count;
			}
		}

		@Override
		public Object getResult() throws NotNumericalException, ValueMismatchException {
			return t.divide(sum, count);
		}

		@Override
		public void reset() {
			sum = null;
		}

		@Override
		public void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException, NotNumericalException, ValueMismatchException {
			AvgAggregator aagg = (AvgAggregator) agg;
			if (aagg.inputCol != inputCol) {
				throw new IllegalArgumentException("Excepted aggregator for column " + inputCol +
						" but received aggregator for colums " + aagg.inputCol);
			}
			count += aagg.count;
			if (sum == null) {
				sum = aagg.sum;
			} else if (aagg.sum != null) {
				sum = t.add(sum, aagg.sum);
			}
		}
	}

	private static class CountAggregator implements Aggregator {
		private final int inputCol;
		private int count;

		CountAggregator(int inputCol) {
			this.inputCol = inputCol;
			count = 0;
		}

		@Override
		public void addTuple(QpTuple<?> t, int count) {
			if (inputCol < 0) {
				this.count += count;
			} else if (! t.isNull(inputCol)) {
				this.count += count;
			}
		}

		@Override
		public Object getResult() {
			return count;
		}

		@Override
		public void reset() {
			count = 0;
		}

		@Override
		public void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException {
			CountAggregator cagg = (CountAggregator) agg;
			if (cagg.inputCol != inputCol) {
				throw new IllegalArgumentException("Excepted aggregator for column " + inputCol + " but received aggregator for column " + cagg.inputCol);
			}
			count += cagg.count;
		}
	}

	private static class MaxAggregator implements Aggregator {
		private final Type t;
		private final int inputCol;
		private Object max;

		MaxAggregator(Type t, int inputCol) {
			this.t = t;
			this.inputCol = inputCol;
			max = null;
		}

		@Override
		public void addTuple(QpTuple<?> t, int count) throws CompareMismatch {
			Object o = t.get(inputCol);
			if (max == null) {
				max = o;
			} else if (o != null && this.t.compareTwo(o, max) > 0) {
				max = o;
			}
		}

		@Override
		public Object getResult() {
			return max;
		}

		@Override
		public void reset() {
			max = null;
		}

		@Override
		public void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException, CompareMismatch {
			MaxAggregator magg = (MaxAggregator) agg;
			if (magg.inputCol != inputCol) {
				throw new IllegalArgumentException("Excepted aggregator for column " + inputCol + " but received aggregator for column " + magg.inputCol);
			}
			if (max == null) {
				max = magg.max;
			} else if (magg.max != null && t.compareTwo(magg.max, max) > 0) {
				max = magg.max;
			}
		}
	}

	private static class MinAggregator implements Aggregator {
		private final Type t;
		private final int inputCol;
		private Object min;

		MinAggregator(Type t, int inputCol) {
			this.t = t;
			this.inputCol = inputCol;
			min = null;
		}

		@Override
		public void addTuple(QpTuple<?> t, int count) throws CompareMismatch {
			Object o = t.get(inputCol);
			if (min == null) {
				min = o;
			} else if (o != null && this.t.compareTwo(o, min) < 0) {
				min = o;
			}
		}

		@Override
		public Object getResult() {
			return min;
		}

		@Override
		public void reset() {
			min = null;
		}

		@Override
		public void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException, CompareMismatch {
			MinAggregator magg = (MinAggregator) agg;
			if (magg.inputCol != inputCol) {
				throw new IllegalArgumentException("Excepted aggregator for column " + inputCol + " but received aggregator for column " + magg.inputCol);
			}
			if (min == null) {
				min = magg.min;
			} else if (magg.min != null && t.compareTwo(magg.min, min) < 0) {
				min = magg.min;
			}
		}
	}

	private static class SumAggregator implements Aggregator {
		private final Type t;
		private final int inputCol;
		private Object sum;

		SumAggregator(Type t, int inputCol) {
			this.t = t;
			this.inputCol = inputCol;
			sum = null;
		}

		@Override
		public void addTuple(QpTuple<?> t, int count) throws NotNumericalException, ValueMismatchException {
			Object o = t.get(inputCol);
			if (count == 1) {
				sum = this.t.add(o, sum);				
			} else {
				sum = this.t.add(this.t.multiply(o, count), sum);
			}
		}

		@Override
		public Object getResult() {
			return sum;
		}

		@Override
		public void reset() {
			sum = null;
		}

		@Override
		public void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException, NotNumericalException, ValueMismatchException {
			SumAggregator sagg = (SumAggregator) agg;
			if (sagg.inputCol != inputCol) {
				throw new IllegalArgumentException("Excepted aggregator for column " + inputCol + " but received aggregator for column " + sagg.inputCol);
			}
			if (sum == null) {
				sum = sagg.sum;
			} else if (sagg.sum != null) {
				sum = t.add(sum, sagg.sum);
			}
		}
	}

	private static class RewrittenAvgAggregator implements Aggregator {
		private final Type t;
		private final int sumCol, countCol;
		private Object sum;
		private int count;

		RewrittenAvgAggregator(Type t, int sumCol, int countCol) {
			this.t = t;
			this.sumCol = sumCol;
			this.countCol = countCol;
			sum = null;
			count = 0;
		}

		@Override
		public void addTuple(QpTuple<?> t, int count) throws NotNumericalException,
		ValueMismatchException {
			Object tSum = t.get(sumCol);
			int tCount = (Integer) t.get(countCol);
			if (count != 1) {
				tSum = this.t.multiply(tSum, count);
				tCount *= count;
			}
			sum = this.t.add(sum, tSum);
			this.count += (Integer) tCount;
		}

		@Override
		public Object getResult() throws NotNumericalException,
		ValueMismatchException {
			return t.divide(sum, count);
		}

		@Override
		public void reset() {
			sum = null;
			count = 0;
		}

		@Override
		public void mergeDataFrom(Aggregator agg) throws ClassCastException, IllegalArgumentException, NotNumericalException, ValueMismatchException {
			RewrittenAvgAggregator ragg = (RewrittenAvgAggregator) agg;
			if (ragg.sumCol != sumCol || ragg.countCol != countCol) {
				throw new IllegalArgumentException("Excepted aggregator for columns " + sumCol + " and " + countCol +
						" but received aggregator for columns " + ragg.sumCol + " and " + ragg.countCol);
			}
			count += ragg.count;
			if (sum == null) {
				sum = ragg.sum;
			} else if (ragg.sum != null) {
				sum = t.add(sum, ragg.sum);
			}
		}
	}

	private InetSocketAddress[] failedNodes = null;
	protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
		QpTupleBag<M> toResend = new QpTupleBag<M>(outputSchema, schemas, mdf);
		int lastPhaseSent = -1;
		int numTuplesDisposed = 0;
		writeLock.lock();
		if (logger.isInfoEnabled()) {
			logger.info("HashAggregator #" + operatorId + " at " + nodeId + " is beginning phase # " + newPhaseNo);
		}
		try {
			for (SynchronizingMap<byte[],GroupData>.Entry e : groups) {
				numTuplesDisposed += e.value.disposeOfFailedNodeTuples(newlyFailedNodes);
			}

			if (outputSchemaIdCols != null) {
				for (SynchronizingMap<byte[],GroupData>.Entry e : groups) {
					RecoverableGroupData rgd = (RecoverableGroupData) e.value;
					rgd.resendTuples(newPhaseNo, newPhaseNo - 1, failedRanges, outputSchemaIdCols, toResend);
				}
			}

			int numFailedNodes = newlyFailedNodes.length;
			if (failedNodes != null) {
				numFailedNodes += failedNodes.length;
			}
			InetSocketAddress[] allFailedNodes = new InetSocketAddress[numFailedNodes];
			int pos = 0;
			if (failedNodes != null) {
				for (InetSocketAddress node : failedNodes) {
					allFailedNodes[pos++] = node;
				}
			}
			for (InetSocketAddress node : newlyFailedNodes) {
				allFailedNodes[pos++] = node;
			}
			this.failedNodes = allFailedNodes;

			if (newPhaseNo >= phasesBegun.length) {
				boolean[] newPhasesBegun = new boolean[newPhaseNo + 1];
				System.arraycopy(phasesBegun, 0, newPhasesBegun, 0, phasesBegun.length);
				phasesBegun = newPhasesBegun;
				phasesBegun[newPhaseNo] = true;
			}
			if (this.lastPhaseFinished >= newPhaseNo) {
				sendAvailablePhases(toResend);
			}

		} catch (CompareMismatch e) {
			this.reportException(e);
		} catch (NotNumericalException e) {
			this.reportException(e);
		} catch (ValueMismatchException e) {
			this.reportException(e);
		} finally {
			writeLock.unlock();
		}
		if (! toResend.isEmpty()) {
			this.sendTuples(toResend);
		}
		if (lastPhaseSent >= 0) {
			this.finishedSending(lastPhaseSent);
		}
		if (logger.isDebugEnabled()) {
			synchronized (this) {
				this.numTuplesDisposed += numTuplesDisposed;
			}
		}
	}

	protected void purgeState(boolean destructive) {
		if (destructive) {
			synchronized (groups) {
				groups.clear();
			}
		}
	}

	private int numTuplesReceived = 0;
	private int numTuplesDisposed = 0;


	private int numTuplesProcessed = 0;
	private int numOtherPhaseTuples = 0;

	private int lastPhaseFinished = -1;
	private boolean[] phasesBegun = new boolean[] { true };

	private int sendAvailablePhases(QpTupleBag<M> tuples) {
		if (lastPhaseFinished < 0) {
			return -1;
		}
		for (int sendingPhase = 0; sendingPhase <= lastPhaseFinished; ++ sendingPhase) {
			if (! phasesBegun[sendingPhase]) {
				return -1;
			}
		}
		numTuplesProcessed = 0;
		int numTuplesStored = 0;
		final int startSize = tuples.size();
		for (SynchronizingMap<byte[],GroupData>.Entry entry : groups) {
			QpTuple<M> output;
			try {
				output = entry.value.getResult(lastPhaseFinished, lastPhaseFinished);
				numTuplesStored += entry.value.getNumTuples();
			} catch (Exception vm) {
				throw new RuntimeException("Couldn't create output tuple", vm);
			}
			if (output != null) {
				tuples.add(output);
			}
		}
		logger.info("Finishing aggregator " + operatorId + " at " + nodeId + " during phase " + lastPhaseFinished + " using " + numTuplesProcessed + " input tuples of " + numTuplesStored + " received tuples (skipping " + this.numOtherPhaseTuples + " later phase tuples) produced " + (tuples.size() - startSize) + " output tuples");
		return lastPhaseFinished;
	}

	@Override
	protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		QpTupleBag<M> tuples = new QpTupleBag<M>(outputSchema, schemas, mdf);

		writeLock.lock();
		numOtherPhaseTuples = 0;
		int lastPhaseSent = -1;
		try {
			if (lastPhaseFinished >= phaseNo) {
				return;
			}
			lastPhaseFinished = phaseNo;
			if (phaseNo >= phasesBegun.length) {
				boolean[] newPhasesBegun = new boolean[phaseNo + 1];
				System.arraycopy(phasesBegun, 0, newPhasesBegun, 0, phasesBegun.length);
				phasesBegun = newPhasesBegun;
				phasesBegun[phaseNo] = false;
			}
			lastPhaseSent = sendAvailablePhases(tuples);
		} finally {
			writeLock.unlock();
		}

		if (! tuples.isEmpty()) {
			sendTuples(tuples);
		}

		if (lastPhaseSent >= 0) {
			this.finishedSending(lastPhaseSent);
		}

		if (logger.isDebugEnabled()) {
			synchronized (this) {
				logger.debug("Aggregator " + this.operatorId + " received " + this.numTuplesReceived + " and disposed of " + this.numTuplesDisposed + " at the end of phase " + phaseNo);
			}
		}
	}
}