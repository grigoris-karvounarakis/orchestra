package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import edu.upenn.cis.orchestra.p2pqp.DHTService.EpochNum;
import edu.upenn.cis.orchestra.p2pqp.Filter.FilterException;
import edu.upenn.cis.orchestra.p2pqp.TupleStore.TupleSink;
import edu.upenn.cis.orchestra.p2pqp.TupleStore.TupleStoreException;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ProbeScanOperator<M> extends Operator<M> implements KeyReceiver {

	private final Filter<? super QpTuple<M>> filter;
	private final TupleStore<M> ts;
	final QpSchema probeSchema;
	private final List<IdRangeSet> phaseRemainingRanges;
	private final List<Set<EpochNum>> alreadyScannedPages;
	private final List<IdRangeSet> alreadyScannedRanges;
	private final byte[] contributingNodes;

	public ProbeScanOperator(QpSchema probeSchema, Filter<? super QpTuple<M>> filter, TupleStore<M> ts, Operator<M> dest,
			WhichInput destInput, InetSocketAddress nodeId, int operatorId, RecordTuples rt, MetadataFactory<M> mdf,
			boolean enableRecovery, IdRangeSet initialRanges) {
		super(dest, destInput, nodeId, operatorId, mdf, ts, rt, enableRecovery);
		this.filter = filter;
		this.ts = ts;
		this.probeSchema = probeSchema;
		alreadyScannedRanges = new ArrayList<IdRangeSet>();
		phaseRemainingRanges = new ArrayList<IdRangeSet>();
		phaseRemainingRanges.add(initialRanges);
		alreadyScannedPages = new ArrayList<Set<EpochNum>>();
		if (enableRecovery) {
			contributingNodes = OutputBuffer.getBytes(nodeId);
		} else {
			contributingNodes = null;
		}
	}

	private synchronized void ensureCapacity(int phaseNo) {
		while (alreadyScannedRanges.size() <= phaseNo) {
			alreadyScannedRanges.add(IdRangeSet.empty());
		}
		while (alreadyScannedPages.size() <= phaseNo) {
			alreadyScannedPages.add(new HashSet<EpochNum>());
		}
		while (phaseRemainingRanges.size() <= phaseNo) {
			phaseRemainingRanges.add(null);
		}
	}

	@Override
	public void receiveKeys(final int phaseNo, EpochNum page, final IdRangeSet keyRanges, KeySource keys) {
		synchronized (this) {
			ensureCapacity(phaseNo);
			if (! alreadyScannedPages.get(phaseNo).add(page)) {
				return;
			}
		}
		final int count = keys.getNumKeys();
		if (count == 0) {
			int finishedPhaseNo = -1;
			synchronized (this) {
				if (phaseRemainingRanges.get(phaseNo) == null) {
					alreadyScannedRanges.get(phaseNo).add(keyRanges);
				} else {
					phaseRemainingRanges.get(phaseNo).remove(keyRanges);
					int lastFinishedPhase = -1;
					for (int i = 0; i < phaseRemainingRanges.size(); ++i) {
						if (phaseRemainingRanges.get(i) != null && phaseRemainingRanges.get(i).isEmpty()) {
							lastFinishedPhase = i;
						} else {
							break;
						}
					}
					if (lastFinishedPhase >= phaseNo) {
						finishedPhaseNo = lastFinishedPhase;
					}
				}
			}
			if (finishedPhaseNo >= 0) {
				finishedSending(finishedPhaseNo);
			}
			keysScanned(keyRanges, phaseNo);
			return;
		}
		Iterator<QpTupleKey> keysIt = keys.getKeys(ts);
		final QpTupleBag<M> scanned = new QpTupleBag<M>(probeSchema, ts, mdf);
		try {
			ts.getTuplesByKey(keysIt, new TupleSink<M>() {

				@Override
				public void deliverTuple(QpTuple<M> tuple) {
					if (filter != null && (! filter.eval(tuple))) {
						return;
					}
					byte[] metadataBytes;
					if (mdf == null) {
						metadataBytes = null;
					} else {
						M metadata = tuple.getMetadata(ts, mdf);
						metadata = mdf.scan(ProbeScanOperator.this, tuple, metadata);
						metadataBytes = mdf.toBytes(metadata);
					}
					scanned.addWhileChanging(tuple, metadataBytes, contributingNodes, phaseNo);
				}

				@Override
				public void tupleNotFound(QpTupleKey key) {
					reportMissing(Collections.singletonList(key), phaseNo);
				}

			});
		} catch (TupleStoreException e) {
			reportException(e);
			return;
		} catch (FilterException e) {
			reportException(e);
			return;
		}
		sendTuples(scanned);
		boolean reportRange = false;
		int finishedPhaseNo = -1;
		synchronized (ProbeScanOperator.this) {
			reportRange = true;
			synchronized (this) {
				if (phaseRemainingRanges.get(phaseNo) == null) {
					alreadyScannedRanges.get(phaseNo).add(keyRanges);
				} else {
					phaseRemainingRanges.get(phaseNo).remove(keyRanges);
					int lastFinishedPhase = -1;
					for (int i = 0; i < phaseRemainingRanges.size(); ++i) {
						if (phaseRemainingRanges.get(i) != null && phaseRemainingRanges.get(i).isEmpty()) {
							lastFinishedPhase = i;
						} else {
							break;
						}
					}
					if (lastFinishedPhase >= phaseNo) {
						finishedPhaseNo = lastFinishedPhase;
					}
				}
			}
		}
		if (reportRange) {
			keysScanned(keyRanges, phaseNo);
		}
		if (finishedPhaseNo >= 0) {
			finishedSending(finishedPhaseNo);
		}
	}

	@Override
	public void beginPhase(int phaseNo, IdRangeSet keyRanges) {
		int finishedPhaseNo = -1;
		synchronized (this) {
			ensureCapacity(phaseNo);
			phaseRemainingRanges.set(phaseNo, keyRanges.clone());
			for (IdRange range : alreadyScannedRanges.get(phaseNo)) {
				phaseRemainingRanges.get(phaseNo).remove(range);
			}
			alreadyScannedRanges.set(phaseNo, null);
			int lastFinishedPhase = -1;
			for (int i = 0; i < phaseRemainingRanges.size(); ++i) {
				if (phaseRemainingRanges.get(i) != null && phaseRemainingRanges.get(i).isEmpty()) {
					lastFinishedPhase = i;
				} else {
					break;
				}
			}
			if (lastFinishedPhase >= phaseNo) {
				finishedPhaseNo = lastFinishedPhase;
			}
		}
		if (finishedPhaseNo >= 0) {
			this.finishedSending(finishedPhaseNo);
		}
	}

	@Override
	protected void receiveTuples(WhichInput destInput, QpTupleBag<M> tuples) {
		throw new UnsupportedOperationException("Should never send tuples to a scan operator");
	}

	@Override
	protected void inputHasFinished(WhichInput whichChild, int phaseNo) {
		throw new UnsupportedOperationException("Should never send tuples to a scan operator");
	}

	@Override
	public int getRelationId() {
		return probeSchema.relId;
	}

}
