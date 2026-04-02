package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.p2pqp.DHTService.EpochNum;
import edu.upenn.cis.orchestra.util.ByteArraySet;

public class DistributedScanOperator<M> extends PullScanOperator<M> implements KeyReceiver {
	private final int relation;
	private final Filter<? super QpTuple<M>> fullFilter;
	private int epoch;
	private final QpApplication<M> app;

	private final Logger logger = Logger.getLogger(this.getClass());
	private boolean interrupted = false;

	private class ScanRecord implements Comparable<ScanRecord> {
		PullScanOperator<M> scan;
		final KeySource keys;
		final IdRangeSet ranges;
		final int phaseNo;
		final EpochNum pageNo;
		int numScanned = 0;

		ScanRecord(KeySource keys, IdRangeSet ranges, int phaseNo, EpochNum pageNo) {
			this.keys = keys;
			this.ranges = ranges;
			this.phaseNo = phaseNo;
			this.pageNo = pageNo;
		}

		void beginScan(ByteArraySet keyHolder) {
			if (keys.getNumKeys() == 0) {
				return;
			}
			if (scan == null) {
				keyHolder.clear();
				keys.addKeysTo(keyHolder);
				if (keyHolder.isEmpty()) {
					return;
				}
				scan = app.store.beginScan(relation, keyHolder, fullFilter, DistributedScanOperator.this, ranges, phaseNo);
			}
		}

		@Override
		public int compareTo(ScanRecord arg0) {
			return ranges.compareTo(arg0.ranges);
		}
	}

	private final List<PriorityQueue<ScanRecord>> storeScans = new ArrayList<PriorityQueue<ScanRecord>>();
	private final Set<ScanRecord> currentScans = new HashSet<ScanRecord>();
	private final List<IdRangeSet> remainingRanges = new ArrayList<IdRangeSet>();
	private final List<IdRangeSet> alreadyScannedRanges = new ArrayList<IdRangeSet>();
	private final List<Set<EpochNum>> alreadyReceived = new ArrayList<Set<EpochNum>>();
	private int numTuplesOutput = 0;
	private int numKeysReceived = 0;

	private boolean closed = false;

	public DistributedScanOperator(int relation,
			QpApplication<M> app,
			Filter<? super QpTuple<M>> fullFilter,
					Operator<M> dest, WhichInput outputDest,
					int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt,
					MetadataFactory<M> mdf, QpSchema.Source schemas, boolean enableRecovery, IdRangeSet initialRanges) {
		super(dest, outputDest, queryId, nodeId, operatorId, rt, mdf, schemas, enableRecovery);
		this.relation = relation;
		this.fullFilter = fullFilter;
		this.app = app;
		remainingRanges.add(initialRanges);
		alreadyScannedRanges.add(null);
	}

	public void resetState(int currentEpoch) {
		if (this.epoch == currentEpoch) {
			logger.error("Wrong epoch to set!");
		}
		this.epoch = currentEpoch;
	}
	@Override
	public synchronized boolean isFinished(int phaseNo) {
		IdRangeSet remaining = remainingRanges.get(phaseNo);
		return remaining != null && remaining.isEmpty();
	}

	@Override
	public int scan(int blockSize, int phaseNo) {
		return scan(blockSize, false, phaseNo);
	}

	@Override
	public int scanAll(int phaseNo) {
		return scan(Integer.MAX_VALUE, true, phaseNo);
	}

	private int scan(int blockSize, boolean all, int phaseNo) {
		ensureCapacityFor(phaseNo);
		ByteArraySet keys = new ByteArraySet(DHTService.numTuplesPerPage, 0.5);
		int numScanned = 0;
		try {
			for ( ; ; ) {
				ScanRecord scan = null;
				synchronized (this) {
					scan = storeScans.get(phaseNo).poll();
					while (scan == null) {
						if (closed || interrupted || remainingRanges.get(phaseNo) != null && remainingRanges.get(phaseNo).isEmpty()) {
							return numScanned;
						}
						wait();
						scan = storeScans.get(phaseNo).poll();
					}
					currentScans.add(scan);
				}
				int scannedThisTime;
				boolean finished;
				if (scan.keys.getNumKeys() == 0) {
					finished = true;
				} else {
					if (scan.scan == null) {
						scan.beginScan(keys);
					}
					if (scan.scan == null) {
						finished = true;
					} else {
						if (all) {
							scannedThisTime = scan.scan.scanAll(phaseNo);
						} else {
							scannedThisTime = scan.scan.scan(blockSize, phaseNo);
						}
						numScanned += scannedThisTime;
						scan.numScanned += scannedThisTime;
						finished = scan.scan.isFinished(phaseNo);
						if (finished) {
							scan.scan.close();
						}
					}
				}
				this.activityHasOccurred();
				if (finished) {
					this.keysScanned(scan.ranges, phaseNo);
				}
				int finishedPhaseNo = -1;
				synchronized (this) {
					currentScans.remove(scan);
					if (scan.scan == null || finished) {
						this.numTuplesOutput += scan.numScanned;
						IdRangeSet ranges = remainingRanges.get(phaseNo);
						if (ranges == null) {
							alreadyScannedRanges.get(phaseNo).add(scan.ranges);
							if (logger.isDebugEnabled()) {
								logger.debug("DistributedScanOperator " + operatorId + " at " + app.localAddr + " in phase " + phaseNo + " scanned " + scan.ranges + " from page " + scan.pageNo);
							}
						} else {
							ranges.remove(scan.ranges);
							if (logger.isDebugEnabled()) {
								logger.debug("DistributedScanOperator " + operatorId + " at " + app.localAddr + " in phase " + phaseNo + " scanned " + scan.ranges + " from page " + scan.pageNo + ", remaining ranges " + ranges);
							}
							if (ranges.isEmpty()) {
								logger.info("DistributedScanOperator " + operatorId + " at " + app.localAddr + " in phase " + phaseNo + " finished scanning");
								notifyAll();
								int lastFinishedPhase = -1;
								for (int i = 0; i < remainingRanges.size(); ++i) {
									if (remainingRanges.get(i) != null && remainingRanges.get(i).isEmpty()) {
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
					} else if (closed || interrupted) {
						scan.scan.close();
					} else {
						storeScans.get(phaseNo).add(scan);
						notifyAll();
					}
				}
				if (finishedPhaseNo >= 0) {
					logger.info("DistributedScanOperator " + operatorId + " at " + app.localAddr + " in phase " + phaseNo + " finished sending, total # tuples sent is " + this.numTuplesOutput + ", total # keys received is " + this.numKeysReceived);
					this.finishedSending(finishedPhaseNo);
				}
				if (! all) {
					return numScanned;
				}
			}
		} catch (InterruptedException ie) {
			reportException(ie);
			return numScanned;
		}
	}

	protected synchronized void close() {
		closed = true;
		notifyAll();
		for (ScanRecord sr : currentScans) {
			if (sr.scan != null) {
				sr.scan.interrupt();
			}
		}
		for (Queue<ScanRecord> scans : storeScans) {
			while (! scans.isEmpty()) {
				ScanRecord scan = scans.remove();
				if (scan.scan != null) {
					scan.scan.close();
				}
			}
		}
	}

	private synchronized void ensureCapacityFor(int phaseNo) {
		while (remainingRanges.size() <= phaseNo) {
			remainingRanges.add(null);
		}
		while (storeScans.size() <= phaseNo) {
			storeScans.add(new PriorityQueue<ScanRecord>());
		}
		while (alreadyReceived.size() <= phaseNo) {
			alreadyReceived.add(new HashSet<EpochNum>());
		}
		while (alreadyScannedRanges.size() <= phaseNo) {
			alreadyScannedRanges.add(IdRangeSet.empty());
		}
	}

	public void receiveKeys(int phaseNo, EpochNum page, IdRangeSet keyRanges, KeySource keys) {
		if (logger.isDebugEnabled()) {
			logger.debug("DistributedScanOperator " + operatorId + " at " + app.localAddr + " in phase " + phaseNo + " received " + keys.getNumKeys() + " keys from page " + page + " in range " + keyRanges);
		}

		synchronized (this) {
			if (closed) {
				return;
			}
			ensureCapacityFor(phaseNo);
			// Don't scan a page multiple times
			if (! alreadyReceived.get(phaseNo).add(page)) {
				return;
			}
			storeScans.get(phaseNo).add(new ScanRecord(keys,keyRanges,phaseNo,page));
			numKeysReceived += keys.getNumKeys();

			notify();
		}
	}

	@Override
	public synchronized void interrupt() {
		List<List<EpochNum>> receivedPages = new ArrayList<List<EpochNum>>(alreadyReceived.size());
		for (Collection<EpochNum> pages : alreadyReceived) {
			List<EpochNum> newPages = new ArrayList<EpochNum>(pages);
			Collections.sort(newPages);
			receivedPages.add(newPages);
		}
		logger.warn("DistributedScanOperator " + operatorId + " at " + app.localAddr + " was interrupted with " + remainingRanges + " remaining and " + receivedPages + " pages received");
		interrupted = true;
		for (ScanRecord sr : currentScans) {
			if (sr.scan != null) {
				sr.scan.interrupt();
			}
		}
		for (Queue<ScanRecord> scans : storeScans) {
			while (! scans.isEmpty()) {
				ScanRecord scan = scans.remove();
				if (scan.scan != null) {
					scan.scan.close();
				}
			}
		}
		notifyAll();
	}

	public void beginPhase(int phaseNo, IdRangeSet keyRanges) {
		logger.info("DistributedScanOperator " + phaseNo + " at " + app.localAddr + " in phase " + phaseNo + " beginning phase for ranges " + keyRanges);
		int finishedPhaseNo = -1;
		synchronized (this) {
			if (remainingRanges.size() > phaseNo && remainingRanges.get(phaseNo) != null) {
				throw new IllegalStateException("Beginning phase " + phaseNo + " multiple times");
			}
			ensureCapacityFor(phaseNo);
			IdRangeSet remainingRanges = keyRanges.clone();
			this.remainingRanges.set(phaseNo, remainingRanges);
			for (IdRange alreadyScanned : alreadyScannedRanges.get(phaseNo)) {
				remainingRanges.remove(alreadyScanned);
			}
			logger.info("DistributedScanOperator " + phaseNo + " at " + app.localAddr + " in phase " + phaseNo + " remaining ranges " + remainingRanges);
			alreadyScannedRanges.set(phaseNo, null);
			if (remainingRanges.isEmpty()) {
				logger.info("DistributedScanOperator " + operatorId + " finished scanning for phase " + phaseNo);
			}
			int lastFinishedPhase = -1;
			for (int i = 0; i < this.remainingRanges.size(); ++i) {
				if (this.remainingRanges.get(i) != null && this.remainingRanges.get(i).isEmpty()) {
					lastFinishedPhase = i;
				} else {
					break;
				}
			}
			if (lastFinishedPhase >= phaseNo) {
				finishedPhaseNo = lastFinishedPhase;
			}
			notifyAll();
		}
		if (finishedPhaseNo >= 0) {
			logger.info("DistributedScanOperator " + operatorId + " at " + app.localAddr + " in phase " + phaseNo + " finished sending, total # tuples sent is " + this.numTuplesOutput + ", # keys received is " + this.numKeysReceived);
			this.finishedSending(finishedPhaseNo);
		}
	}

	@Override
	public boolean rescanDuringRecovery() {
		return true;
	}

	@Override
	public int getRelationId() {
		return relation;
	}
}
