package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public abstract class SpoolOperator<M> extends Operator<M> {
	SpoolOperator(InetSocketAddress nodeId, int operatorId,
			RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas, boolean allowRecovery) {
		super(nodeId, operatorId, mdf, schemas, rt, allowRecovery);
	}

	public static <M> SpoolOperator<M> create(QpSchema schema, QpSchema.Source schemas, InetSocketAddress nodeId, int operatorId,
			RecordTuples rt, MetadataFactory<M> mdf, boolean allowRecovery, boolean discardResults) {
		if (mdf == null && discardResults && (! allowRecovery)) {
			return new CountingSpoolOperator<M>(nodeId, operatorId, rt);
		} else if (mdf == null) {
			return new NoMetadataSpoolOperator<M>(nodeId, operatorId, rt, allowRecovery, schema);
		} else if (allowRecovery) {
			return new RecoverableMetadataSpoolOperator<M>(nodeId, operatorId, rt, mdf, schemas);
		} else {
			return new MetadataSpoolOperator<M>(nodeId, operatorId, rt, mdf, schemas);
		}
	}

	abstract int getResultCardinality();

	abstract List<QpTuple<M>> getResultsWithMetadata();	
	abstract List<QpTuple<Null>> getResultsWithoutMetadata();

	@Override
	synchronized protected final void inputHasFinished(WhichInput whichChild, int phaseNo) {
		if (phaseNo > lastFinishedPhase) {
			notifyAll();
			lastFinishedPhase = phaseNo;
		}
	}

	private int lastFinishedPhase = -1;
	private int currentPhase = 0;
	private boolean failed = false;

	synchronized void failed() {
		this.failed = true;
		this.notifyAll();
	}

	synchronized void currentPhaseIs(int phaseNo) {
		this.currentPhase = phaseNo;
		this.notifyAll();
	}

	synchronized void waitUntilFinished() throws InterruptedException {
		while (! (failed || lastFinishedPhase >= currentPhase)) {
			wait();
		}
	}

	synchronized boolean hasFinished() {
		return (lastFinishedPhase >= currentPhase);
	}

	synchronized boolean hasFailed() {
		return failed;
	}
}


class CountingSpoolOperator<M> extends SpoolOperator<M> {
	private int count = 0;

	CountingSpoolOperator(InetSocketAddress nodeId, int operatorId, RecordTuples rt) {
		super(nodeId, operatorId, rt, null, null, false);
	}

	@Override
	synchronized int getResultCardinality() {
		return count;
	}

	@Override
	List<QpTuple<M>> getResultsWithMetadata() {
		throw new UnsupportedOperationException();
	}

	@Override
	List<QpTuple<Null>> getResultsWithoutMetadata() {
		throw new UnsupportedOperationException();
	}

	@Override
	synchronized protected void receiveTuples(WhichInput dest, QpTupleBag<M> tuples) {
		count += tuples.size();
	}
}

class NoMetadataSpoolOperator<M> extends SpoolOperator<M> {
	private QpTupleBag<Null> tuples;
	private ByteArraySet failedNodes;


	NoMetadataSpoolOperator(InetSocketAddress nodeId, int operatorId, RecordTuples rt,
			boolean allowRecovery, QpSchema schema) {
		super(nodeId, operatorId, rt, null, null, allowRecovery);

		tuples = new QpTupleBag<Null>(schema, null, null);
	}

	@SuppressWarnings("unchecked")
	@Override
	synchronized List<QpTuple<M>> getResultsWithMetadata() {
		ArrayList<QpTuple<M>> retval = new ArrayList<QpTuple<M>>();
		for (QpTuple<Null> t : tuples) {
			retval.add((QpTuple<M>) t);
		}
		return retval;
	}

	@Override
	synchronized List<QpTuple<Null>> getResultsWithoutMetadata() {
		ArrayList<QpTuple<Null>> retval = new ArrayList<QpTuple<Null>>();
		for (QpTuple<Null> t : tuples) {
			retval.add(t);
		}
		return retval;
	}

	@Override
	synchronized int getResultCardinality() {
		return tuples.size();
	}


	@SuppressWarnings("unchecked")
	@Override
	synchronized protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		if (failedNodes == null) {
			this.tuples.addFrom((QpTupleBag<Null>) tuples);
		} else {
			this.tuples.addFromDroppingFailed((QpTupleBag<Null>) tuples, failedNodes);
		}
	}

	@Override
	public void close() {
		tuples = null;
	}

	@Override
	synchronized protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
		if (this.failedNodes == null) {
			this.failedNodes = new ByteArraySet(100);
		}
		for (InetSocketAddress node : newlyFailedNodes) {
			this.failedNodes.add(OutputBuffer.getBytes(node));
		}
		QpTupleBag<Null> newTuples = new QpTupleBag<Null>(tuples.schema, tuples.findSchema, tuples.mdf, tuples.length());
		newTuples.addFromDroppingFailed(tuples, this.failedNodes);
		tuples = newTuples;
	}
}

class MetadataSpoolOperator<M> extends SpoolOperator<M> {
	private Map<QpTuple<Null>,M> tuples;

	MetadataSpoolOperator(InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas) {
		super(nodeId, operatorId, rt, mdf, schemas, false);
		tuples = new HashMap<QpTuple<Null>,M>();
	}

	@Override
	synchronized List<QpTuple<M>> getResultsWithMetadata() {
		List<QpTuple<M>> retval = new ArrayList<QpTuple<M>>(tuples.size());

		for (Map.Entry<QpTuple<Null>, M> me : tuples.entrySet()) {
			retval.add(me.getKey().changeMetadata(me.getValue(), mdf));
		}

		return retval;
	}

	@Override
	synchronized List<QpTuple<Null>> getResultsWithoutMetadata() {
		List<QpTuple<Null>> retval = new ArrayList<QpTuple<Null>>(tuples.size());

		for (Map.Entry<QpTuple<Null>, M> me : tuples.entrySet()) {
			final QpTuple<Null> t = me.getKey();
			final int cardinality = mdf.getCardinality(me.getValue());
			for (int i = 0; i < cardinality; ++i) {
				retval.add(t);
			}
		}

		return retval;
	}

	@Override
	synchronized int getResultCardinality() {
		int retval = 0;
		for (M metadata : tuples.values()) {
			retval += mdf.getCardinality(metadata);
		}
		return retval;
	}


	@Override
	synchronized protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		for (QpTuple<M> t : tuples) {
			QpTuple<Null> nullT = t.changeMetadata(null, null);
			if (this.tuples.containsKey(nullT)) {
				M currMetadata = this.tuples.get(nullT);
				M newMetadata = mdf.addMetadata(t.getMetadata(schemas,mdf), currMetadata);
				if (mdf.isZero(newMetadata)) {
					this.tuples.remove(nullT);
				} else {
					this.tuples.put(nullT, newMetadata);
				}
			} else {
				this.tuples.put(nullT, t.getMetadata(schemas,mdf));
			}
		}
	}

	@Override
	protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
		throw new IllegalStateException("Recovery was not enabled at operator creation time");
	}
}

class RecoverableMetadataSpoolOperator<M> extends SpoolOperator<M> {
	private final Map<TupleAndNodeIds,M> metadata = new HashMap<TupleAndNodeIds,M>();
	
	RecoverableMetadataSpoolOperator(InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf, QpSchema.Source schemas) {
		super(nodeId, operatorId, rt, mdf, schemas, true);
	}

	@Override
	synchronized List<QpTuple<M>> getResultsWithMetadata() {
		Map<QpTuple<Null>,M> unified = unify();
		ArrayList<QpTuple<M>> retval = new ArrayList<QpTuple<M>>(unified.size());
		for (Map.Entry<QpTuple<Null>,M> me : unified.entrySet()) {
			retval.add(me.getKey().changeMetadata(me.getValue(),mdf));
		}
		return retval;
	}

	@Override
	synchronized List<QpTuple<Null>> getResultsWithoutMetadata() {
		Map<QpTuple<Null>,M> unified = unify();
		ArrayList<QpTuple<Null>> retval = new ArrayList<QpTuple<Null>>(unified.size());
		for (Map.Entry<QpTuple<Null>,M> me : unified.entrySet()) {
			final QpTuple<Null> t = me.getKey();
			final int card = mdf.getCardinality(me.getValue());
			for (int i = 0; i < card; ++i) {
				retval.add(t);
			}
		}
		return retval;
	}


	@Override
	int getResultCardinality() {
		int retval = 0;
		for (M metadata : this.metadata.values()) {
			retval += mdf.getCardinality(metadata);
		}
		return retval;
	}

	synchronized private Map<QpTuple<Null>,M> unify() {
		HashMap<QpTuple<Null>,M> retval = new HashMap<QpTuple<Null>,M>();
		for (Map.Entry<TupleAndNodeIds, M> me : metadata.entrySet()) {
			final M metadata = me.getValue();
			final QpTuple<Null> t = me.getKey().t;
			if (retval.containsKey(t)) {
				M origMetadata = retval.get(t);
				M newMetadata = mdf.addMetadata(metadata, origMetadata);
				if (mdf.isZero(newMetadata)) {
					retval.remove(t);
				} else {
					retval.put(t, newMetadata);
				}
			} else {
				retval.put(t, metadata);
			}
		}
		return retval;
	}

	@Override
	synchronized protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		for (QpTuple<M> t : tuples) {
			TupleAndNodeIds tani = new TupleAndNodeIds(t);
			if (metadata.containsKey(tani)) {
				M newMetadata = mdf.addMetadata(t.getMetadata(schemas,mdf), metadata.get(tani));
				if (mdf.isZero(newMetadata)) {
					metadata.remove(tani);
				} else {
					metadata.put(tani, newMetadata);
				}
			} else {
				metadata.put(tani, t.getMetadata(schemas,mdf));
			}
		}
	}

	@Override
	synchronized protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
		throw new IllegalStateException("Need to implement");
	}
}
