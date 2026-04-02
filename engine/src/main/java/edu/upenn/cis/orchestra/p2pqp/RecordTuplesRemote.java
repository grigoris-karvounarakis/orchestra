package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.p2pqp.messages.DoesNotHaveQuery;
import edu.upenn.cis.orchestra.p2pqp.messages.RecordTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySendingFailed;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySuccess;

public class RecordTuplesRemote implements RecordTuples {
	final private QpApplication<?> app;
	final private SendThread sendThread;
	final private InetSocketAddress dest;
	final private int queryId;

	private final Logger logger = Logger.getLogger(this.getClass());

	private Set<Long> outstandingMessages = Collections.synchronizedSet(new HashSet<Long>());
	private List<Long> sentMessages = Collections.synchronizedList(new ArrayList<Long>());

	private volatile boolean activityHasOccurred = false;

	RecordTuplesRemote(QpApplication<?> app, int sendDelayMs, InetSocketAddress dest, int queryId) {
		this.dest = dest;
		this.queryId = queryId;
		this.app = app;
		sendThread = new SendThread(sendDelayMs);
		sendThread.start();
	}

	@Override
	public void close() {
		try {
			sendThread.interrupt();
			sendThread.join();
		} catch (InterruptedException ie) {
			logger.error("Interrupted while closing RecordTuplesRemote for query " + queryId, ie);
			return;
		}

		scanned.clear();
		exceptions.clear();
		failed.clear();
		missing.clear();
		finished.clear();
		sentMessages.clear();
		synchronized (outstandingMessages) {
			for (long msgId : outstandingMessages) {
				app.removeReplyContinuation(msgId);
			}
			outstandingMessages.clear();
		}
		logger.info("Closed RecordTuplesRemote for query " + queryId);
	}

	private class SendThread extends Thread {
		final int sendDelayMs;
		SendThread(int sendDelayMs) {
			super(app.tg, "RecordTuplesRemote SendThread(" + queryId + "," + app.localAddr + ")");
			this.sendDelayMs = sendDelayMs;
		}

		public void run() {
			try {
				while (! isInterrupted()) {
					Thread.sleep(sendDelayMs);
					List<Scanned> currScanned;
					List<Exception> currExceptions;
					Set<InetSocketAddress> currFailed;
					List<OperatorAndPhase> currFinished;
					List<Missing> currMissing;

					synchronized (RecordTuplesRemote.this) {
						if (scanned.isEmpty()) {
							currScanned = Collections.emptyList();
						} else {
							currScanned = scanned;
							scanned = new ArrayList<Scanned>();
						}
						if (exceptions.isEmpty()) {
							currExceptions = Collections.emptyList();
						} else {
							currExceptions = exceptions;
							exceptions = new ArrayList<Exception>();
						}
						if (failed.isEmpty()) {
							currFailed = Collections.emptySet();
						} else {
							currFailed = new HashSet<InetSocketAddress>(failed);
							failed.clear();
						}
						if (finished.isEmpty()) {
							currFinished = Collections.emptyList();
						} else {
							currFinished = finished;
							finished = new ArrayList<OperatorAndPhase>();
						}
						if (missing.isEmpty()) {
							currMissing = Collections.emptyList();
						} else {
							currMissing = missing;
							missing = new ArrayList<Missing>();
						}
					}
					Map<Integer,IdRangeSet> scanned = new HashMap<Integer,IdRangeSet>();
					Map<OperatorAndPhase,List<QpTupleKey>> missing = new HashMap<OperatorAndPhase,List<QpTupleKey>>();

					for (Scanned s : currScanned) {
						IdRangeSet scannedForOp = scanned.get(s.operatorId);
						if (scannedForOp == null) {
							scannedForOp = IdRangeSet.empty();
							scanned.put(s.operatorId, scannedForOp);
						}
						scannedForOp.add(s.range);
					}

					for (Missing m : currMissing) {
						OperatorAndPhase oap = new OperatorAndPhase(m.operatorId, m.phaseNo);
						List<QpTupleKey> missingForOp = missing.get(oap);
						if (missingForOp == null) {
							missingForOp = new ArrayList<QpTupleKey>();
							missing.put(oap, missingForOp);
						}
						missingForOp.add(m.key);
					}

					if (scanned.isEmpty() && missing.isEmpty() && currExceptions.isEmpty() && currFailed.isEmpty() && currFinished.isEmpty() && (! activityHasOccurred)) {
						continue;
					}

					activityHasOccurred = false;

					final RecordTuplesMessage rtm = new RecordTuplesMessage(dest, scanned, currExceptions, currFailed, missing, currFinished, queryId);
					outstandingMessages.add(rtm.messageId);
					sentMessages.add(rtm.messageId);
					try {
						app.sendMessageAwaitReply(rtm, new ReplyContinuation() {
							boolean finished = false;
							public synchronized boolean isFinished() {
								return finished;
							}

							public synchronized void processReply(QpMessage m) {
								finished = true;
								outstandingMessages.remove(rtm.messageId);
								if ((m instanceof DoesNotHaveQuery) || (m instanceof ReplySendingFailed)) {
									return;
								} else if (! (m instanceof ReplySuccess)) {
									Exception e = new Exception("Error sending RecordTuplesMessage to " + dest + ": " + m);
									logger.error(e);
								}
							}
						}, ReplySuccess.class);
					} catch (IOException e) {
						logger.error(e);
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}
	}

	private static class Scanned {
		final int operatorId;
		final IdRange range;

		Scanned(int operatorId, IdRange range) {
			this.operatorId = operatorId;
			this.range = range;
		}
	}

	private List<Scanned> scanned = new ArrayList<Scanned>();
	private List<Exception> exceptions = new ArrayList<Exception>();
	private List<InetSocketAddress> failed = new ArrayList<InetSocketAddress>();

	private List<OperatorAndPhase> finished = new ArrayList<OperatorAndPhase>();

	private static class Missing {
		final int operatorId;
		final int phaseNo;
		final QpTupleKey key;

		Missing(int operatorId, int phaseNo, QpTupleKey key) {
			this.operatorId = operatorId;
			this.phaseNo = phaseNo;
			this.key = key;
		}
	}

	private List<Missing> missing = new ArrayList<Missing>();

	@Override
	public synchronized void keysScanned(int operatorId, IdRange range) {
		scanned.add(new Scanned(operatorId, range));
	}

	@Override
	public synchronized void reportException(Exception e) {
		exceptions.add(e);
	}

	@Override
	public synchronized void keysNotFound(int operatorId, Collection<QpTupleKey> keys, int phaseNo) {
		for (QpTupleKey key : keys) {
			missing.add(new Missing(operatorId, phaseNo, key));
		}
	}

	@Override
	public synchronized void nodesHaveFailed(Collection<InetSocketAddress> nodes) {
		failed.addAll(nodes);
	}

	@Override
	public synchronized void operatorHasFinished(int operatorId, int phaseNo) {
		finished.add(new OperatorAndPhase(operatorId,phaseNo));
	}

	int getOutstandingMessages() {
		return outstandingMessages.size();
	}

	int getSentMessages() {
		return sentMessages.size();
	}

	@Override
	public void activityHasOccurred() {
		activityHasOccurred = true;
	}
}
