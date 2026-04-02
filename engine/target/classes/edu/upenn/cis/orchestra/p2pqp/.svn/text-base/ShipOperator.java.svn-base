package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.p2pqp.messages.EndOfStreamMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.MessageDestHasDied;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySendingFailed;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySuccess;
import edu.upenn.cis.orchestra.p2pqp.messages.ShippedTuplesMessage;
import edu.upenn.cis.orchestra.util.LongList;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ShipOperator<M> extends Operator<M> {
	final private SendThread sendThread;
	final private QpApplication<M> app;
	final private QueryExecution<M> exec;
	final private InetSocketAddress dest;
	final private int queryId;

	private int lastFinishedPhase = -1;
	private int lastFinishedPhaseSent = -1;
	private int lastBegunPhase = 0;

	final private QpSchema newSchema;
	final private AbstractRelation.RelationMapping newSchemaMapping;
	final private String namedDest;
	private final Router nonRecoveryRouter;
	private final int destOperator;
	private final WhichInput destInput;

	private final Map<InetSocketAddress,LongList> sentShipMessages;
	private final LongList destSentShipMessages;

	private final Logger logger = Logger.getLogger(this.getClass());

	private Set<Long> outstandingMessages = Collections.synchronizedSet(new HashSet<Long>());

	final byte[] nodeIdBytes;

	private class BufferedTuples {
		final QpTupleBag<M> sent;
		final int beingResentPhase;

		BufferedTuples() {
			sent = new QpTupleBag<M>(newSchema, schemas, mdf);
			beingResentPhase = -1;
		}

		BufferedTuples(int beingResentPhase) {
			sent = null;
			this.beingResentPhase = beingResentPhase;
		}

	}

	private static class NodeAndPhase {
		final InetSocketAddress node;
		final int phase;

		NodeAndPhase(InetSocketAddress node, int phase) {
			this.node = node;
			this.phase = phase;
		}

		public boolean equals(Object o) {
			NodeAndPhase nap = (NodeAndPhase) o;
			return (phase == nap.phase && node.equals(nap.node));
		}

		public int hashCode() {
			return node.hashCode() + 37 * phase;
		}

		public String toString() {
			return phase + "@" + node;
		}
	}

	private final Map<NodeAndPhase,BufferedTuples> bufferedTuples;
	private QpTupleBag<M> pendingToShip;
	private QpTupleBag<M> fromUnbegunPhases;

	private static class SentMessageInfo {
		final long msgId;
		final InetSocketAddress dest;
		final Map<Set<InetSocketAddress>,Integer> relevantNodesCounts;

		SentMessageInfo(long msgId, InetSocketAddress dest, Map<Set<InetSocketAddress>,Integer> relevantNodesCounts) {
			this.msgId = msgId;
			this.dest = dest;
			this.relevantNodesCounts = relevantNodesCounts;
		}
	}

	private List<SentMessageInfo> sentMessageInfo = new ArrayList<SentMessageInfo>();

	public ShipOperator(QpApplication<M> app, QueryExecution<M> exec, QpSchema outputSchema, InetSocketAddress dest, String namedDest, int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, Map<Integer,int[]> recoveryOperators, int destOperator, WhichInput destInput) throws ValueMismatchException {
		this(app, exec, null, outputSchema, null, dest, namedDest, queryId, nodeId, operatorId, rt, recoveryOperators, destOperator, destInput);
	}

	public ShipOperator(QpApplication<M> app, QueryExecution<M> exec, QpSchema oldSchema, QpSchema newSchema, int[] newToOldPos,  InetSocketAddress dest, String namedDest, int queryId, InetSocketAddress nodeId, int operatorId, RecordTuples rt, Map<Integer,int[]> recoveryOperators, int destOperator, WhichInput destInput) throws ValueMismatchException {
		super(nodeId, operatorId, null, null, rt, recoveryOperators != null);
		this.destOperator = destOperator;
		this.destInput = destInput;
		this.dest = dest;
		if (dest == null) {
			destSentShipMessages = null;
			sentShipMessages = new HashMap<InetSocketAddress,LongList>();
		} else {
			destSentShipMessages = new LongList();
			sentShipMessages = null;
		}
		if (dest == null && (! enableRecovery)) {
			nonRecoveryRouter = exec.getMostRecentRouter();
		} else {
			nonRecoveryRouter = null;
		}
		final boolean bufferTuples = enableRecovery ? (recoveryOperators.get(this.operatorId) != null) : false;
		this.queryId = queryId;
		this.app = app;
		this.exec = exec;
		this.newSchema = newSchema;
		if (oldSchema == null) {
			this.newSchemaMapping = null;
		} else {
			AbstractRelation.FieldSource newSchemaMapping[] = new AbstractRelation.FieldSource[newSchema.getNumCols()];
			if (newToOldPos == null) {
				throw new IllegalArgumentException("Need new schema mapping");
			} else if (newToOldPos.length != newSchema.getNumCols()) {
				throw new IllegalArgumentException("Schema mapping should one entry for each column in the new schema");
			}
			for (int i = 0; i < newToOldPos.length; ++i) {
				newSchemaMapping[i] = new AbstractRelation.FieldSource(newToOldPos[i],true);
			}
			this.newSchemaMapping = new AbstractRelation.RelationMapping(oldSchema, newSchema, null, newSchemaMapping);
		}
		this.namedDest = namedDest;
		final int shipIntervalMs = exec.config.sendDelayMs;
		if (shipIntervalMs > 0) {
			pendingToShip = new QpTupleBag<M>(newSchema, null, null);
			sendThread = new SendThread(shipIntervalMs);
			sendThread.start();
		} else {
			sendThread = null;
			pendingToShip = null;
		}
		if (bufferTuples) {
			bufferedTuples = new HashMap<NodeAndPhase,BufferedTuples>();
		} else {
			bufferedTuples = null;
		}
		if (enableRecovery) {
			fromUnbegunPhases = new QpTupleBag<M>(newSchema, null, null);
		} else {
			fromUnbegunPhases = null;
		}

		this.nodeIdBytes = OutputBuffer.getBytes(nodeId);
	}

	private void shipTuples(QpTupleBag<M> input) {
		if (input.isEmpty()) {
			return;
		}

		Map<InetSocketAddress,Map<Set<InetSocketAddress>,Integer>> currentRelevantSentCountMap;
		Map<InetSocketAddress,Long> destMessageId;
		if (logger.isDebugEnabled() && this.enableRecovery) {
			currentRelevantSentCountMap = new HashMap<InetSocketAddress,Map<Set<InetSocketAddress>,Integer>>();
			destMessageId = new HashMap<InetSocketAddress,Long>();
		} else {
			currentRelevantSentCountMap = null;
			destMessageId = null;
		}
		Map<InetSocketAddress,int[]> sentCounts;
		if (dest == null) {
			Map<InetSocketAddress,QpTupleBag<M>> toRehash = new HashMap<InetSocketAddress,QpTupleBag<M>>();
			final Router[] routers = exec.getRouters();
			// Doing a rehash
			sentCounts = new HashMap<InetSocketAddress,int[]>();
			if (enableRecovery) {
				Iterator<QpTuple<M>> it = input.recyclingIterator();
				while (it.hasNext()) {
					QpTuple<M> t = it.next();
					Id id = t.getQPid();
					InetSocketAddress dest = routers[t.getPhase()].getDest(id);
					if (failedNodes.contains(dest)) {
						continue;
					}
					QpTupleBag<M> forDest = toRehash.get(dest);
					if (forDest == null) {
						forDest = new QpTupleBag<M>(newSchema, null, null);
						toRehash.put(dest, forDest);
					}
					forDest.add(t.addContributingNode(dest));
					int[] sent = sentCounts.get(dest);
					if (sent == null) {
						sent = new int[lastBegunPhase + 1];
						sentCounts.put(dest, sent);
					}
					++sent[t.getPhase()];
					if (currentRelevantSentCountMap != null) {
						Set<InetSocketAddress> contributingNodes = new HashSet<InetSocketAddress>();
						t.addContributingNodesTo(contributingNodes);
						Map<Set<InetSocketAddress>,Integer> currentRelevantSentCount = currentRelevantSentCountMap.get(dest);
						if (currentRelevantSentCount == null) {
							currentRelevantSentCount = new HashMap<Set<InetSocketAddress>,Integer>();
							currentRelevantSentCountMap.put(dest, currentRelevantSentCount);
						}
						Integer count = currentRelevantSentCount.get(contributingNodes);
						if (count == null) {
							count = 0;
						}
						++count;
						currentRelevantSentCount.put(contributingNodes, count);
					}
				}
			} else {
				Iterator<QpTuple<M>> it = input.recyclingIterator();
				while (it.hasNext()) {
					QpTuple<M> t = it.next();
					Id id = t.getQPid();
					InetSocketAddress dest = nonRecoveryRouter.getDest(id);
					QpTupleBag<M> forDest = toRehash.get(dest);
					if (forDest == null) {
						forDest = new QpTupleBag<M>(newSchema, null, null);
						toRehash.put(dest, forDest);
					}
					forDest.add(t);
					int[] sent = sentCounts.get(dest);
					if (sent == null) {
						sent = new int[1];
						sentCounts.put(dest, sent);
					}
					++sent[0];
					if (currentRelevantSentCountMap != null) {
						Set<InetSocketAddress> contributingNodes = new HashSet<InetSocketAddress>();
						t.addContributingNodesTo(contributingNodes);
						Map<Set<InetSocketAddress>,Integer> currentRelevantSentCount = currentRelevantSentCountMap.get(dest);
						if (currentRelevantSentCount == null) {
							currentRelevantSentCount = new HashMap<Set<InetSocketAddress>,Integer>();
							currentRelevantSentCountMap.put(dest, currentRelevantSentCount);
						}
						Integer count = currentRelevantSentCount.get(contributingNodes);
						if (count == null) {
							count = 0;
						}
						++count;
						currentRelevantSentCount.put(contributingNodes, count);
					}
				}
			}
			input = null;
			for (Map.Entry<InetSocketAddress, QpTupleBag<M>> me : toRehash.entrySet()) {
				final InetSocketAddress dest = me.getKey();
				final QpTupleBag<M> tuples = me.getValue();		

				ShippedTuplesMessage stm = new ShippedTuplesMessage(dest, null, true, queryId, tuples, destOperator, destInput, exec.config.compressionLevel);
				synchronized (sentShipMessages) {
					LongList msgIds = sentShipMessages.get(dest);
					if (msgIds == null) {
						msgIds = new LongList();
						sentShipMessages.put(dest, msgIds);
					}
					msgIds.add(stm.messageId);
				}
				if (destMessageId != null) {
					destMessageId.put(dest, stm.messageId);
				}
				try {
					app.totalOutputShippingMessage += stm.getDataLength();
					outstandingMessages.add(stm.messageId);
					app.sendMessageAwaitReply(stm, new ShipReplyContinuation(stm.messageId, dest, true), ReplySuccess.class);
				} catch (IOException e) {
					logger.fatal(e);
					reportException(e);
				} catch (InterruptedException ie) {
					return;
				}
			}
		} else {
			if (enableRecovery) {
				input = input.addContributingNode(nodeIdBytes);
			}
			int[] phaseCounts = new int[this.lastBegunPhase + 1];
			Iterator<QpTuple<M>> it = input.recyclingIterator();
			while (it.hasNext()) {
				QpTuple<M> t = it.next();
				++phaseCounts[t.getPhase()];
			}
			if (currentRelevantSentCountMap != null) {
				it = input.recyclingIterator();
				while (it.hasNext()) {
					Set<InetSocketAddress> contributingNodes = new HashSet<InetSocketAddress>();
					it.next().addContributingNodesTo(contributingNodes);
					Map<Set<InetSocketAddress>,Integer> currentRelevantSentCount = currentRelevantSentCountMap.get(dest);
					if (currentRelevantSentCount == null) {
						currentRelevantSentCount = new HashMap<Set<InetSocketAddress>,Integer>();
						currentRelevantSentCountMap.put(dest, currentRelevantSentCount);
					}
					Integer count = currentRelevantSentCount.get(contributingNodes);
					if (count == null) {
						count = 0;
					}
					++count;
					currentRelevantSentCount.put(contributingNodes, count);
				}
			}
			sentCounts = Collections.singletonMap(dest, phaseCounts);

			ShippedTuplesMessage ship = new ShippedTuplesMessage(dest, namedDest, false, queryId, input, destOperator, destInput, exec.config.compressionLevel);
			synchronized (destSentShipMessages) {
				destSentShipMessages.add(ship.messageId);
			}
			if (destMessageId != null) {
				destMessageId.put(dest, ship.messageId);
			}
			try {
				app.totalOutputShippingMessage += ship.getDataLength();
				outstandingMessages.add(ship.messageId);
				app.sendMessageAwaitReply(ship, new ShipReplyContinuation(ship.messageId, dest, true), ReplySuccess.class);
			} catch (IOException e) {
				logger.fatal(e);
				reportException(e);
			} catch (InterruptedException ie) {
				return;
			}
		}
		int minPhase = Integer.MAX_VALUE;
		synchronized (sentCount) {
			for (Map.Entry<InetSocketAddress, int[]> me : sentCounts.entrySet()) {
				final InetSocketAddress node = me.getKey();
				final int[] counts = me.getValue();
				for (int phase = 0; phase < counts.length; ++phase) {
					NodeAndPhase nap = new NodeAndPhase(node,phase);
					Integer count = sentCount.get(nap);
					if (count == null) {
						count = 0;
					}
					count += counts[phase];
					if (counts[phase] > 0 && phase < minPhase) {
						minPhase = phase;
					}
					sentCount.put(nap, count);
				}
			}
		}
		synchronized (this) {
			if (minPhase <= this.lastFinishedPhaseSent) {
				reportException(new RuntimeException("Sending tuple from phase " + minPhase + " when last finished phase sent is " + this.lastFinishedPhaseSent));
			}
		}
		if (currentRelevantSentCountMap != null) {
			synchronized (this.sentMessageInfo) {
				for (Map.Entry<InetSocketAddress, Map<Set<InetSocketAddress>, Integer>> me : currentRelevantSentCountMap.entrySet()) {
					final InetSocketAddress dest = me.getKey();
					final long msgId = destMessageId.get(dest);
					sentMessageInfo.add(new SentMessageInfo(msgId, dest, me.getValue()));
				}
			}
		}

	}

	private class ShipReplyContinuation extends ReplyContinuation {
		private boolean finished = false;

		private final long msgId;
		private final boolean isTuples;
		private final InetSocketAddress dest;

		ShipReplyContinuation(long msgId, InetSocketAddress dest, boolean isTuples) {
			this.msgId = msgId;
			this.isTuples = isTuples;
			this.dest = dest;
		}

		public synchronized boolean isFinished() {
			return finished;
		}

		public synchronized void processReply(QpMessage m) {
			finished = true;

			if (! outstandingMessages.remove(msgId)) {
				logger.warn("Didn't find ship message ID " + msgId  + " in set of outstanding messages");
			}
			if (m instanceof ReplySuccess) {
			} else if ((m instanceof MessageDestHasDied) || (m instanceof ReplySendingFailed)) {
				reportNodeFailure(dest);
			} else {
				if (! failed) {
					Exception e = new Exception("Could not deliver " + (isTuples ? "ShippedTuplesMessage" : "EndOfStreamMessage") + " #" + m.messageId + " to " + dest + " for operator " + operatorId + " of query " + queryId + " at " + app.localAddr + ": " + m);
					logger.fatal("Could not deliver tuples", e);
					reportException(e);
				}
				failed = true;
			}
		}

	}

	private volatile boolean failed = false;

	private final Map<NodeAndPhase,Integer> sentCount = new HashMap<NodeAndPhase,Integer>();

	private void logSentMessageInfo() {
		int survivingCount = 0;
		int failedCount = 0;
		synchronized (this.sentMessageInfo) {
			for (SentMessageInfo smi : sentMessageInfo) {
				int thisMessageSurvivingCount = 0;
				int thisMessageFailedCount = 0;
				COUNT: for (Map.Entry<Set<InetSocketAddress>, Integer> me : smi.relevantNodesCounts.entrySet()) {
					for (InetSocketAddress failed : this.failedNodes) {
						if (me.getKey().contains(failed)) {
							thisMessageFailedCount += me.getValue();
							continue COUNT;
						}
					}
					thisMessageSurvivingCount += me.getValue();
				}
				survivingCount += thisMessageSurvivingCount;
				failedCount += thisMessageFailedCount;
				if (logger.isTraceEnabled()) {
					logger.trace("ShipOperator " + operatorId + " at " + nodeId + " sent as ShippedTupleMessage #" + smi.msgId + " to " + smi.dest + " " + thisMessageSurvivingCount + " surviving tuples and " + thisMessageFailedCount + " failed tuples");
				}
			}
		}
		logger.debug("ShipOperator " + operatorId + " at " + nodeId + " sent " + survivingCount + " surviving tuples and " + failedCount + " failed tuples");

	}

	protected void close() {
		if (logger.isInfoEnabled()) {
			synchronized (sentCount) {
				if (logger.isDebugEnabled()) {
					logger.debug("ShipOperator " + operatorId + " at " + nodeId + " sent the following numbers of tuples: " + sentCount);
				}
				if (logger.isInfoEnabled()) {
					int failedCount = 0, survivedCount = 0;
					for (Map.Entry<NodeAndPhase, Integer> me : sentCount.entrySet()) {
						if (this.failedNodes.contains(me.getKey().node)) {
							failedCount += me.getValue();
						} else {
							survivedCount += me.getValue();
						}
					}
					logger.info("ShipOperator " + operatorId + " at " + nodeId + " sent " + survivedCount + " tuples to surviving nodes and " + failedCount + " tuples to failed nodes");
				}
			}
		}
		if (logger.isDebugEnabled()) {
			logSentMessageInfo();
			if (! outstandingMessages.isEmpty()) {
				logger.debug("ShipOperator " + operatorId + " at " + nodeId + " is closing having not received acknowledgements for shipped tuples messages: " + this.outstandingMessages);
			}
		}
		if (pendingToShip != null) {
			pendingToShip.clear();
		}
		if (sendThread != null) {
			try {
				sendThread.interrupt();
				sendThread.join();
			} catch (InterruptedException ie) {
				return;
			}
		}
		synchronized (outstandingMessages) {
			for (long msgId : outstandingMessages) {
				app.removeReplyContinuation(msgId);
			}
			outstandingMessages.clear();
		}
	}

	private class SendThread extends Thread {
		private final int sendIntervalMs;
		SendThread(int sendIntervalMs) {
			super(app.tg, "ShipOperator(" + queryId + "," + operatorId + "," + app.localAddr + ")");
			this.sendIntervalMs = sendIntervalMs;
		}

		public void run() {
			while (! isInterrupted()) {
				try {
					synchronized (this) {
						wait(sendIntervalMs);
					}
				} catch (InterruptedException ie) {
					return;
				}
				final QpTupleBag<M> toSend;
				int phaseToSend = -1;
				synchronized (ShipOperator.this) {
					if (pendingToShip.isEmpty()) {
						toSend = null;
					} else {
						toSend = pendingToShip;
						pendingToShip = new QpTupleBag<M>(newSchema, null, null);
					}
					if ((toSend == null || toSend.isEmpty()) && lastFinishedPhase == lastFinishedPhaseSent) {
						continue;
					}
					if (lastFinishedPhase > lastFinishedPhaseSent && lastFinishedPhase <= lastBegunPhase) {
						phaseToSend = lastFinishedPhase;
					}
				}
				if (toSend != null) {
					shipTuples(toSend);
				}
				if (phaseToSend >= 0) {
					try {
						sendPhaseFinished(phaseToSend);
					} catch (Exception e) {
						logger.fatal(e);
						reportException(e);
					}
				}
			}
		}
	}

	int getOutstandingMessages() {
		return outstandingMessages.size();
	}

	@Override
	protected void receiveTuples(WhichInput dest, QpTupleBag<M> tuples) {
		QpTupleBag<M> toSend;
		if (newSchemaMapping == null) {
			toSend = tuples;
		} else {
			toSend = tuples.applyMapping(newSchemaMapping, newSchema);
			tuples.clear();
		}
		deliverTuples(toSend);
	}

	private void deliverTuples(QpTupleBag<M> toSend) {
		if (this.enableRecovery) {
			QpTupleBag<M> fromBegunPhases = new QpTupleBag<M>(newSchema, null, null, toSend.length());
			synchronized (this) {
				Iterator<QpTuple<M>> it = toSend.recyclingIterator();
				while (it.hasNext()) {
					QpTuple<M> t = it.next();
					final int phase = t.getPhase();
					if (phase <= this.lastFinishedPhaseSent) {
						this.reportException(new RuntimeException("ShipOperator #" + this.operatorId + " at " + this.nodeId + " received tuple from phase " + phase + " when last finished phase is " + this.lastFinishedPhase));
						return;
					} else if (phase <= lastBegunPhase) {
						fromBegunPhases.add(t);
					} else {
						fromUnbegunPhases.add(t);
					}
				}
				toSend = fromBegunPhases;
				if (bufferedTuples != null) {
					QpTupleBag<M> toSendInCorrectPhase = new QpTupleBag<M>(newSchema, null, null, toSend.length());
					it = toSend.recyclingIterator();
					while (it.hasNext()) {
						QpTuple<M> t = it.next();
						Id id = t.getQPid();
						for ( ; ; ) {
							Router r = exec.getRouter(t.getPhase());
							InetSocketAddress tupleDest = r.getDest(id);
							NodeAndPhase nap = new NodeAndPhase(tupleDest, t.getPhase());
							BufferedTuples bt = bufferedTuples.get(nap);
							if (bt == null) {
								bt = new BufferedTuples();
								bufferedTuples.put(nap, bt);
							}
							if (bt.beingResentPhase >= 0) {
								t = t.changePhase(bt.beingResentPhase);
								continue;
							} else {
								bt.sent.add(t);
								toSendInCorrectPhase.add(t);
								break;
							}
						}
					}
					toSend = toSendInCorrectPhase;
				}
			}
		}
		if (sendThread == null) {
			shipTuples(toSend);
		} else {
			synchronized (this) {
				pendingToShip.addFrom(toSend);	
			}
		}
		toSend.clear();
	}

	@Override
	protected synchronized void inputHasFinished(WhichInput whichChild, int phaseNo) {
		if (lastFinishedPhase >= phaseNo) {
			this.reportException(new RuntimeException("Trying to finished phase " + phaseNo + " but phase " + lastFinishedPhase + " has already finished"));
			return;
		}
		lastFinishedPhase = phaseNo;
		if (sendThread != null) {
			// Wake up the send thread
			synchronized (sendThread) {
				sendThread.notify();
			}
		} else if (sendThread == null && phaseNo <= lastBegunPhase) {
			try {
				sendPhaseFinished(phaseNo);
			} catch (Exception e) {
				logger.fatal(e);
				reportException(e);
			}
		}
	}

	private Set<InetSocketAddress> failedNodes = Collections.synchronizedSet(new HashSet<InetSocketAddress>());

	protected void beginNewPhase(int newPhaseNo, InetSocketAddress[] newlyFailedNodes, IdRangeSet failedRanges) throws InterruptedException {
		QpTupleBag<M> toSend = new QpTupleBag<M>(newSchema, null, null);
		for (InetSocketAddress failed : newlyFailedNodes) {
			failedNodes.add(failed);
		}
		synchronized (this) {
			this.lastBegunPhase = newPhaseNo;
			QpTupleBag<M> stillUnbegunTuples = new QpTupleBag<M>(newSchema, null, null);
			for (QpTuple<M> t : fromUnbegunPhases) {
				if (t.getPhase() <= newPhaseNo) {
					toSend.add(t);
				} else {
					stillUnbegunTuples.add(t);
				}
			}
			this.fromUnbegunPhases = stillUnbegunTuples;

			if (bufferedTuples != null) {
				int resentCount = 0;
				for (InetSocketAddress node : newlyFailedNodes) {
					for (int phase = 0; phase < newPhaseNo; ++phase) {
						NodeAndPhase nap = new NodeAndPhase(node,phase);
						BufferedTuples bt = bufferedTuples.get(nap);
						if (bt != null && bt.sent != null) {
							Iterator<QpTuple<M>> it = bt.sent.recyclingIterator();
							while (it.hasNext()) {
								QpTuple<M> t = it.next();
								toSend.addWhileChangingPhase(t, newPhaseNo);
							}
							resentCount += bt.sent.size();
						}
						bufferedTuples.put(nap, new BufferedTuples(newPhaseNo));
					}
				}
				if (logger.isInfoEnabled()) {
					logger.info("ShipOperator " + operatorId + " at " + nodeId + " in phase " + newPhaseNo + " is resending " + resentCount + " tuples");
				}
			}
			if (! toSend.isEmpty()) {
				deliverTuples(toSend);
			}
		}
		if (this.sendThread == null) {
			try {
				synchronized (this) {
					if (lastFinishedPhase <= newPhaseNo && lastFinishedPhase > lastFinishedPhaseSent) {
						sendPhaseFinished(lastFinishedPhase);
					}
				}
			} catch (Exception e) {
				logger.fatal(e);
				reportException(e);
			}
		} else {
			synchronized (this.sendThread) {
				this.sendThread.notify();
			}
		}
	}


	private void sendPhaseFinished(int phaseNo) throws IOException, InterruptedException {
		if (phaseNo < 0) {
			throw new IllegalArgumentException("Phase number must be positive: " + phaseNo);
		}
		synchronized (this) {
			if (this.lastFinishedPhaseSent >= phaseNo) {
				throw new IllegalStateException("Trying to send phase " + phaseNo + " finished when last finished phase sent is currently " + this.lastFinishedPhaseSent);
			}
			this.lastFinishedPhaseSent = phaseNo;
		}
		if (logger.isInfoEnabled()) {
			logger.info("ShipOperator " + operatorId + " at " + nodeId + " in phase " + phaseNo + " finished receiving input and sending output");
		}
		if (dest == null) {
			List<QpMessage> msgs = new ArrayList<QpMessage>();
			final Set<InetSocketAddress> eosDests;
			if (this.enableRecovery) {
				eosDests = exec.getRouter(phaseNo).getParticipants();
			} else {
				eosDests = this.nonRecoveryRouter.getParticipants();
			}
			for (InetSocketAddress eosDest : eosDests) {
				if (failedNodes.contains(eosDest)) {
					continue;
				}
				synchronized (sentShipMessages) {
					LongList sent = sentShipMessages.get(eosDest);
					if (sent == null) {
						sent = new LongList();
						sentShipMessages.put(eosDest, sent);
					}
					QpMessage m = new EndOfStreamMessage(eosDest, null, true, queryId, sent, phaseNo, destOperator, destInput);
					if (logger.isDebugEnabled()) {
						logger.debug("ShipOperator " + operatorId + " at " + nodeId + " in phase " + phaseNo + " sent EOS message to " + eosDest + " with previous messages " + sent);
					}
					sent.add(m.messageId);
					msgs.add(m);
				}
			}
			for (QpMessage m : msgs) {
				outstandingMessages.add(m.messageId);
				app.sendMessageAwaitReply(m, new ShipReplyContinuation(m.messageId, m.getDest(), false), ReplySuccess.class);
			}
		} else {
			QpMessage m;
			synchronized (destSentShipMessages) {
				m = new EndOfStreamMessage(dest, namedDest, false, queryId, destSentShipMessages, phaseNo, destOperator, destInput);
				if (logger.isDebugEnabled()) {
					logger.debug("ShipOperator " + operatorId + " at " + nodeId + " in phase " + phaseNo + " sent EOS message to " + dest + " with previous messages " + destSentShipMessages);
				}
				destSentShipMessages.add(m.messageId);
			}
			outstandingMessages.add(m.messageId);
			app.sendMessageAwaitReply(m, new ShipReplyContinuation(m.messageId, dest, false), ReplySuccess.class);
		}
		this.finishedSending(phaseNo);
	}
}
