package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpApplication.RecoveryMode;
import edu.upenn.cis.orchestra.p2pqp.messages.BeginNewQueryPhase;
import edu.upenn.cis.orchestra.p2pqp.messages.EndOfStreamMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.PushBufferMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.QueryExecutionMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyFailure;
import edu.upenn.cis.orchestra.p2pqp.messages.ScanTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ShippedTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.plan.Location;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan.OperatorAndDest;
import edu.upenn.cis.orchestra.util.LongSet;
import edu.upenn.cis.orchestra.util.Pair;

public class QueryExecution<M> implements QpSchema.Source {
	final QpApplication.Configuration config;

	public static final int statusIntervalMs = 1000;
	public final int queryId;
	private final Map<Integer,Operator<M>> operators;
	private final List<ShipOperator<?>> shipOperators;
	public final QpApplication<M> app;
	public final int epoch;

	private final InetSocketAddress owner;

	private final RecordTuples rt;
	private final List<Thread> startedThreads = Collections.synchronizedList(new ArrayList<Thread>());

	private final Map<Integer,QpSchema> schemas;
	private final Map<String,QpSchema> schemasByName;

	private static final Logger logger = Logger.getLogger(QueryExecution.class);
	private final boolean isDistributed;
	private final String namedExec;

	private final Map<Integer,int[]> recoveryOperators;

	private final Set<InetSocketAddress> failedNodes = Collections.synchronizedSet(new HashSet<InetSocketAddress>()); 

	private final Map<OperatorAndDest,Location> shipInputLocs;

	private long initialSentDataCount;

	private SpoolOperator<M> spoolOperator;

	private final List<BufferedTuples> bufferedTuples;

	private class BufferedTuples {
		private QpTupleBag<M> tuples;
		private final int destOperator;
		private final Operator.WhichInput destInput;
		private final List<NodeAndId> includedMessages = new ArrayList<NodeAndId>();
		private final List<Set<InetSocketAddress>> receivedFinishedMessages = new ArrayList<Set<InetSocketAddress>>();

		private boolean sendImmediately = false;

		BufferedTuples(int destOperator, Operator.WhichInput destInput) {
			this.destOperator = destOperator;
			this.destInput = destInput;
		}

		void receive(ShippedTuplesMessage stm) {
			synchronized (this) {
				includedMessages.add(new NodeAndId(stm.getOrigin(), stm.messageId));
				QpTupleBag<M> msgTuples = stm.getTuples(QueryExecution.this, app.mdf);
				if (tuples == null) {
					tuples = msgTuples;
				} else {
					tuples.addFrom(msgTuples);
				}
			}
			deliver(true);
		}

		void receive(EndOfStreamMessage eos) {
			synchronized (this) {
				while (receivedFinishedMessages.size() <= eos.phaseNo) {
					receivedFinishedMessages.add(new HashSet<InetSocketAddress>());
				}
				Set<InetSocketAddress> finishedForPhase = receivedFinishedMessages.get(eos.phaseNo); 
				finishedForPhase.add(eos.getOrigin());
				if (finishedForPhase.size() == getRouter(eos.phaseNo).size()) {
					sendImmediately = true;					
				}
			}
			deliver(true);
		}

		void deliver(boolean optional) {
			QpTupleBag<M> toDeliver = null;
			List<NodeAndId> toDeliverMsgs = null;
			synchronized (this) {
				if (sendImmediately || (! optional) || (tuples != null && tuples.length() >= config.maxBufferedLength)) {
					toDeliver = tuples;
					tuples = null;
					if (! includedMessages.isEmpty()) {
						toDeliverMsgs = new ArrayList<NodeAndId>(includedMessages);
						includedMessages.clear();
					}
				}
			}
			if (toDeliver != null) {
				if (logger.isInfoEnabled()) {
					logger.info("Sending operator " + destOperator + " input " + destInput + " " + toDeliver.length() + " bytes of data");
				}
				operators.get(destOperator).receiveTuples(destInput, toDeliver);
			}
			if (toDeliverMsgs != null) {
				for (NodeAndId nai : toDeliverMsgs) {
					recordShipMessageProcessed(nai.node, nai.id);
				}
			}
		}
	}

	private static class NodeAndId {
		final InetSocketAddress node;
		final long id;

		NodeAndId(InetSocketAddress node, long id) {
			this.node = node;
			this.id = id;
		}
	}

	public String toString() {
		String start = "QueryExecution("+queryId +",";
		if (isDistributed) {
			return start + "distributed)";
		} else if (namedExec != null) {
			return start + namedExec + ")";
		} else {
			return start + "centralized)";
		}
	}

	private final List<Router> routers = new ArrayList<Router>();
	private final List<TimerTask> startedTasks = new ArrayList<TimerTask>();

	QueryExecution(Router queryRouter, int epoch, int queryId, Collection<QpSchema> schemas, QpApplication<M> app, InetSocketAddress queryOwner, String locName, QueryPlan<M> qp, QpApplication.Configuration config) {
		this(queryRouter, epoch, queryId,schemas,app,new RecordTuplesRemote(app, statusIntervalMs / 4, queryOwner, queryId),queryOwner, false, locName, qp, config);
	}

	QueryExecution(Router queryRouter, int epoch, int queryId, Collection<QpSchema> schemas, QpApplication<M> app, InetSocketAddress queryOwner, boolean isDistributed, QueryPlan<M> qp, QpApplication.Configuration config) {
		this(queryRouter, epoch, queryId,schemas,app,new RecordTuplesRemote(app, statusIntervalMs / 4, queryOwner, queryId),queryOwner, isDistributed, null, qp, config);
	}

	QueryExecution(Router queryRouter, int epoch, int queryId, Collection<QpSchema> schemas, QpApplication<M> app, QueryOwner<M> rt, QueryPlan<M> qp, QpApplication.Configuration config) {
		this(queryRouter, epoch, queryId,schemas,app,rt,app.localAddr, false, null,  qp, config);
	}

	private QueryExecution(Router queryRouter, int epoch, int queryId, Collection<QpSchema> schemas, QpApplication<M> app, RecordTuples rt, InetSocketAddress owner, boolean isDistributed, String namedExec, QueryPlan<M> qp, QpApplication.Configuration config) {
		routers.add(queryRouter);

		this.config = config;

		this.isDistributed = isDistributed;
		this.namedExec = namedExec;
		this.epoch = epoch;
		this.queryId = queryId;
		this.owner = owner;
		this.app = app;

		initialSentDataCount = app.socketManager.getTotalBytesSent();

		operators = new HashMap<Integer,Operator<M>>();
		shipOperators = new ArrayList<ShipOperator<?>>();

		this.rt = rt;

		this.schemas = new HashMap<Integer,QpSchema>(schemas.size());
		this.schemasByName = new HashMap<String,QpSchema>(schemas.size());
		for (QpSchema s : schemas) {
			this.schemas.put(s.relId, s);
			this.schemasByName.put(s.getName(), s);
		}

		if (config.recoveryMode == RecoveryMode.INCREMENTAL) {
			Map<Integer,int[]> recoveryOperators = new HashMap<Integer,int[]>();
			qp.getRecoveryResendOperators(recoveryOperators, this);
			this.recoveryOperators = Collections.unmodifiableMap(recoveryOperators);
		} else {
			recoveryOperators = null;
		}

		Map<OperatorAndDest,Location> shipInputLocs = new HashMap<OperatorAndDest,Location>();
		qp.getShipInputLocations(shipInputLocs);
		this.shipInputLocs = Collections.unmodifiableMap(shipInputLocs);
		if (config.bufferReceivedTuples) {
			int max = -1;
			for (OperatorAndDest oad : shipInputLocs.keySet()) {
				if (oad.hashCode() > max) {
					max = oad.hashCode();
				}
			}
			this.bufferedTuples = new ArrayList<BufferedTuples>(max+1);
			for (int i = 0; i < max + 1; ++i) {
				bufferedTuples.add(null);
			}
			for (OperatorAndDest oad : shipInputLocs.keySet()) {
				this.bufferedTuples.set(oad.hashCode(), new BufferedTuples(oad.operator, oad.dest));
				if (config.maxBufferedWaitMs > 0) {
					startedTasks.add(app.scheduleDeliverRepeatedMessage(new PushBufferMessage(this.isDistributed, this.namedExec, this.queryId, oad.operator, oad.dest), config.maxBufferedWaitMs));
				}
			}
		} else {
			this.bufferedTuples = null;
		}
	}

	synchronized long close() throws InterruptedException {
		for (TimerTask tt : startedTasks) {
			tt.cancel();
		}
		for (Thread t : startedThreads) {
			t.interrupt();
			t.join();
		}
		logger.info("Closed all started threads for " + toString());
		for (Operator<?> o : operators.values()) {
			o.close();
		}
		logger.info("Closed all operators for " + toString());
		rt.close();
		logger.info("Closed RecordTuples for " + toString());

		final long bytesSent = app.socketManager.getTotalBytesSent() - this.initialSentDataCount;
		System.out.println("Sent " + bytesSent + " during execution of query " + this.queryId + " at " + this.app.localAddr);

		if (logger.isDebugEnabled()) {
			Map<OperatorAndDest,Map<InetSocketAddress,Integer>> survivingCounts = new HashMap<OperatorAndDest,Map<InetSocketAddress,Integer>>();
			Map<OperatorAndDest,Map<InetSocketAddress,Integer>> failedCounts = new HashMap<OperatorAndDest,Map<InetSocketAddress,Integer>>();

			for (Map.Entry<OperatorDestOrigin, Map<Set<InetSocketAddress>,Integer>> me : this.receivedTupleCounts.entrySet()) {
				final OperatorDestOrigin odo = me.getKey();
				final OperatorAndDest od = new OperatorAndDest(odo.operatorId, odo.dest);
				int survivingCount = 0, failedCount = 0;
				COUNT: for (Map.Entry<Set<InetSocketAddress>, Integer> countMe : me.getValue().entrySet()) {
					for (InetSocketAddress failed : this.failedNodes) {
						if (countMe.getKey().contains(failed)) {
							failedCount += countMe.getValue();
							continue COUNT;
						}
					}
					survivingCount += countMe.getValue();
				}
				Map<InetSocketAddress,Integer> survivingCountsForOD = survivingCounts.get(od);
				if (survivingCountsForOD == null) {
					survivingCountsForOD = new HashMap<InetSocketAddress,Integer>();
					survivingCounts.put(od, survivingCountsForOD);
				}
				survivingCountsForOD.put(odo.origin, survivingCount);

				Map<InetSocketAddress,Integer> failedCountsForOD = failedCounts.get(od);
				if (failedCountsForOD == null) {
					failedCountsForOD = new HashMap<InetSocketAddress,Integer>();
					failedCounts.put(od, failedCountsForOD);
				}
				failedCountsForOD.put(odo.origin, failedCount);
			}

			for (OperatorAndDest od : survivingCounts.keySet()) {
				int totalSurvivingCount = 0, totalFailedCount = 0;
				for (Integer count : survivingCounts.get(od).values()) {
					totalSurvivingCount += count;
				}
				for (Integer count : failedCounts.get(od).values()) {
					totalFailedCount += count;
				}
				logger.debug("Operator " + od + " at " + app.localAddr + " received " + totalSurvivingCount + " " + survivingCounts.get(od) + " surviving tuples and " + totalFailedCount + " " + failedCounts.get(od) + " failed tuples");
			}
		}

		return bytesSent;
	}

	void setInitialSentDataCount(QueryExecution<M> previousQuery) {
		this.initialSentDataCount = previousQuery.initialSentDataCount;
	}

	void setInitialSentDataCount(long initialSentDataCount) {
		this.initialSentDataCount = initialSentDataCount;
	}

	synchronized void start() {
		Thread t = new Thread(app.tg, QueryExecution.this + " Status Thread") {
			public void run() {
				final String desc;
				if (namedExec != null) {
					desc = namedExec;
				} else if (isDistributed) {
					desc = app.localAddr.toString();
				} else {
					desc = "central";
				}
				while (! isInterrupted()) {
					try {
						Thread.sleep(statusIntervalMs);
					} catch (InterruptedException ie) {
						return;
					}
					if (logger.isDebugEnabled()) {
						if (rt instanceof RecordTuplesRemote) {
							RecordTuplesRemote rtr = (RecordTuplesRemote) rt;
							int outstanding = rtr.getOutstandingMessages();
							int sent = rtr.getSentMessages();
							StringBuilder msg = new StringBuilder("RecordTuplesRemote for " + queryId + " (" + desc + ") has sent " + sent + " messages");
							if (outstanding > 0) {
								msg.append(" and has " + outstanding + " outstanding messages");
							}
							logger.debug(msg.toString());
						} else if (rt instanceof QueryOwner) {
							QueryOwner<?> qo = (QueryOwner<?>) rt;
							logger.debug("RecordTuplesMessages received for query " + queryId + ": " + qo.getMessagesReceived());
						}
					}
					int shippedMessages = 0;
					for (ShipOperator<?> so : shipOperators) {
						shippedMessages += so.getOutstandingMessages();
					}
					if (shippedMessages > 0) {
						logger.info("Ship operators for " + queryId + " (" + desc + ") " +
								" have " + shippedMessages + " outstanding messages");
					}
				}
			}
		};

		t.start();
		startedThreads.add(t);

		startScans(0);
	}

	void continueEpoch(int currentEpoch) {
		int phaseNo = 0;
		List<PullScanOperator<M>> pullScanOperators = new ArrayList<PullScanOperator<M>>();
		for (Operator<M> o : operators.values()) {
			if (o instanceof PullScanOperator) {
				PullScanOperator<M> op = (PullScanOperator<M>) o;
				if (op instanceof DistributedScanOperator) {
					DistributedScanOperator<M> dso = (DistributedScanOperator<M>) op;
					dso.resetState(currentEpoch);
					pullScanOperators.add(dso);
				}
			}
		}
		if (config.multipleScanThreads) {
			for (PullScanOperator<M> o : pullScanOperators) {
				ScanTuplesThread stt = new ScanTuplesThread(Collections.singleton(o), Integer.toString(o.operatorId), phaseNo);
				stt.start();
				startedThreads.add(stt);
			}
		} else {
			ScanTuplesThread stt = new ScanTuplesThread(pullScanOperators, "all", phaseNo);
			stt.start();
			startedThreads.add(stt);
		}
	}

	private synchronized void startScans(int phaseNo) {
		List<PullScanOperator<M>> pullScanOperators = new ArrayList<PullScanOperator<M>>();
		for (Operator<M> o : operators.values()) {
			if (o instanceof PullScanOperator) {
				PullScanOperator<M> pso = (PullScanOperator<M>) o;
				pullScanOperators.add(pso);
			}
		}
		if (config.multipleScanThreads) {
			for (PullScanOperator<M> o : pullScanOperators) {
				if (phaseNo > 0 && (! o.rescanDuringRecovery())) {
					o.finishedSending(phaseNo);
					continue;
				}
				int numThreads = (o instanceof DistributedScanOperator) ? config.threadsPerDistributedScan : 1;
				for (int i = 0; i < numThreads; ++i) {
					ScanTuplesThread stt = new ScanTuplesThread(Collections.singleton(o), Integer.toString(o.operatorId), phaseNo);
					stt.start();
					startedThreads.add(stt);
				}
			}
		} else {
			ScanTuplesThread stt = new ScanTuplesThread(pullScanOperators, "all", phaseNo);
			stt.start();
			startedThreads.add(stt);
		}
	}

	private Set<OperatorAndPhase> startedScans = Collections.synchronizedSet(new HashSet<OperatorAndPhase>());
	private Set<OperatorAndPhase> finishedScans = Collections.synchronizedSet(new HashSet<OperatorAndPhase>());

	private class ScanTuplesThread extends Thread {
		private final Queue<Pair<PullScanOperator<M>,Integer>> toScan;
		private int phaseNo = -1;
		private PullScanOperator<M> currScan;
		private boolean interruptable = true;
		private boolean interrupted = false;
		ScanTuplesThread(Collection<PullScanOperator<M>> toScan, String opIds, int phaseNo) {
			super(app.tg, QueryExecution.this + " ScanTuplesThread " + opIds + " Phase " + phaseNo);
			this.toScan = new ArrayDeque<Pair<PullScanOperator<M>,Integer>>(toScan.size());
			for (PullScanOperator<M> o : toScan) {
				this.toScan.add(new Pair<PullScanOperator<M>,Integer>(o,phaseNo));
			}
			this.setPriority(config.scanThreadPriority);
		}

		public synchronized boolean isInterrupted() {
			return interrupted;
		}

		public void run() {
			for ( ; ; ) {
				if (isInterrupted()) {
					return;
				}
				try {
					Pair<PullScanOperator<M>,Integer> o;
					o = toScan.poll();
					if (o == null) {
						return;
					}
					phaseNo = o.getSecond();
					currScan = o.getFirst();
					interruptable = false;
					final OperatorAndPhase oap = new OperatorAndPhase(currScan.operatorId, phaseNo);
					if (logger.isInfoEnabled() && startedScans.add(oap)) {
						if (isDistributed) {
							logger.info("PullScanOperator " + currScan.operatorId + " at " + app.localAddr + " is starting for phase " + phaseNo);
						} else {
							logger.info("PullScanOperator " + currScan.operatorId + " at central node is starting for phase " + phaseNo);
						}
					}
					if (phaseNo == 0 || currScan.rescanDuringRecovery()) {
						currScan.scanAll(phaseNo);
					} else {
						currScan.finishedSending(phaseNo);
					}
					if (logger.isInfoEnabled() && finishedScans.add(oap)) {
						if (isDistributed) {
							logger.info("PullScanOperator " + currScan.operatorId + " at " + app.localAddr + " has finished for phase " + phaseNo);
						} else {
							logger.info("PullScanOperator " + currScan.operatorId + " at central node has finished for phase " + phaseNo);
						}
					}
					synchronized (this) {
						interruptable = true;
						currScan = null;
						phaseNo = -1;
					}
				} catch (Exception e) {
					rt.reportException(e);
				}
			}
		}

		public synchronized void interrupt() {
			if (interruptable) {
				super.interrupt();
			}
			if (interrupted) {
				return;
			}
			interrupted = true;
			PullScanOperator<M> currScan = this.currScan;
			if (currScan != null) {
				currScan.interrupt();
				currScan.close();
			}
		}
	}

	public void addOperator(Operator<M> o) {
		operators.put(o.operatorId, o);
		if (o instanceof ShipOperator) {
			shipOperators.add((ShipOperator<?>) o);
		}
		if (o instanceof SpoolOperator) {
			spoolOperator = (SpoolOperator<M>) o;
		}
	}

	public RecordTuples getRecordTuples() {
		return rt;
	}

	public QpSchema getSchema(int relationId) {
		QpSchema s = schemas.get(relationId);
		if (s == null) {
			throw new IllegalArgumentException("Relation with ID " + relationId + " is not known for query " + queryId);
		}
		return s;
	}

	public QpSchema getSchema(String name) {
		QpSchema s = schemasByName.get(name);
		if (s == null) {
			throw new IllegalArgumentException("Relation " + name + " is not known for query " + queryId);
		}
		return s;
	}

	public DHTService<M> getDHT() {
		return app.dht;
	}

	public TupleStore<M> getLocalStore() {
		return app.store;
	}

	// Lock for all three of these is processedShippedMessages
	private Map<InetSocketAddress,LongSet> processedShippedMessages = new HashMap<InetSocketAddress,LongSet>();
	private Map<InetSocketAddress,List<FinishedStream>> pendingFinishedStreams = new HashMap<InetSocketAddress,List<FinishedStream>>();
	private Map<OperatorDestPhase,Set<InetSocketAddress>> finishedStreams = new HashMap<OperatorDestPhase,Set<InetSocketAddress>>();

	private static class OperatorDestPhase {
		final int operatorId;
		final WhichInput dest;
		final int phase;

		OperatorDestPhase(int operatorId, WhichInput dest, int phase) {
			this.operatorId = operatorId;
			this.dest = dest;
			this.phase = phase;
		}

		public boolean equals(Object o) {
			OperatorDestPhase odp = (OperatorDestPhase) o;
			return operatorId == odp.operatorId && dest == odp.dest && phase == odp.phase;
		}

		public int hashCode() {
			int hashCode = operatorId + 37 * phase;
			if (dest != null) {
				hashCode += 127 * dest.ordinal();
			}
			return hashCode;
		}

		public String toString() {
			return "(" + operatorId + "," + dest + "," + phase + ")";
		}
	}

	private static class OperatorAndPhase {
		final int operator;
		final int phase;

		OperatorAndPhase(int operator, int phase) {
			this.operator = operator;
			this.phase = phase;
		}

		public int hashCode() {
			return operator + 127 * phase;
		}

		public boolean equals(Object o) {
			OperatorAndPhase oap = (OperatorAndPhase) o;
			return (oap.operator == operator && oap.phase == phase);
		}

		public String toString() {
			return "(" + operator + "," + phase + ")";
		}
	}

	private static class OperatorDestOrigin {
		final int operatorId;
		final WhichInput dest;
		final InetSocketAddress origin;

		OperatorDestOrigin(int operatorId, WhichInput dest, InetSocketAddress origin) {
			this.operatorId = operatorId;
			this.dest = dest;
			this.origin = origin;
		}

		public boolean equals(Object o) {
			OperatorDestOrigin odo = (OperatorDestOrigin) o;
			return operatorId == odo.operatorId && dest == odo.dest  && origin.equals(odo.origin);
		}

		public int hashCode() {
			int hashCode = operatorId + 37 * origin.hashCode();
			if (dest != null) {
				hashCode += 127 * dest.ordinal();
			}
			return hashCode;
		}

		public String toString() {
			return "(" + operatorId + "," + dest + "," + origin + ")";
		}
	}

	private static class FinishedStream {
		final LongSet remainingMessages;
		final EndOfStreamMessage m;

		FinishedStream(EndOfStreamMessage m, LongSet remainingMessages) {
			this.m = m;
			this.remainingMessages = remainingMessages;
		}

		OperatorDestPhase getODP() {
			return new OperatorDestPhase(m.destOperator, m.destInput, m.phaseNo);
		}
	}

	private void recordShipMessageProcessed(final InetSocketAddress origin, final long messageId) {
		List<OperatorDestPhase> toSend = new ArrayList<OperatorDestPhase>();
		List<EndOfStreamMessage> processed = new ArrayList<EndOfStreamMessage>();

		synchronized (processedShippedMessages) {
			LongSet received = processedShippedMessages.get(origin);
			if (received == null) {
				received = new LongSet();
				processedShippedMessages.put(origin, received);
			}
			received.add(messageId);
			List<FinishedStream> finished = pendingFinishedStreams.get(origin);
			if (finished != null && (! finished.isEmpty())) {
				Iterator<FinishedStream> it = finished.iterator();
				while (it.hasNext()) {
					FinishedStream fs = it.next();
					if (fs.remainingMessages.remove(messageId) && fs.remainingMessages.isEmpty()) {
						if (logger.isDebugEnabled()) {
							logger.debug("Node " + this.app.localAddr + " processed EOS for operator " + fs.m.destOperator  + " dest " + fs.m.destInput + " phase " + fs.m.phaseNo + " from " + origin);
						}
						it.remove();
						processed.add(fs.m);
						OperatorDestPhase odp = fs.getODP();
						Set<InetSocketAddress> finishedForODP = finishedStreams.get(odp);
						if (finishedForODP == null) {
							finishedForODP = new HashSet<InetSocketAddress>();
							finishedStreams.put(odp, finishedForODP);
						}
						if (finishedForODP.add(origin)) {
							Set<InetSocketAddress> senders = this.getSenders(odp); 
							if (senders != null && finishedForODP.equals(senders)) {
								toSend.add(odp);
							}
						}
					}
				}
			}
		}
		for (OperatorDestPhase odp : toSend) {
			sendEndOfStream(odp.operatorId, odp.dest, odp.phase);
		}
		rt.activityHasOccurred();
		for (EndOfStreamMessage eos : processed) {
			recordShipMessageProcessed(eos.getDest(), eos.messageId);
		}
	}

	private Set<InetSocketAddress> getSenders(OperatorDestPhase odp) {
		Location l = shipInputLocs.get(new OperatorAndDest(odp.operatorId, odp.dest));
		if (l.isCentralized()) {
			return Collections.singleton(this.owner);
		} else if (l.isNamed()) {
			return Collections.singleton(this.getNamedNode(l.getName()));
		} else {
			Router r = this.getRouter(odp.phase);
			if (r == null) {
				return null;
			} else {
				return r.getParticipants();
			}
		}
	}

	private void sendEndOfStream(int operatorId, WhichInput inputDest, int phaseNo) {
		Operator<?> o = operators.get(operatorId);
		if (logger.isDebugEnabled()) {
			logger.debug("QueryExecution is sending EOS to operator " + operatorId + " dest " + inputDest + " phase " + phaseNo);
		}
		o.inputHasFinished(inputDest, phaseNo);
	}

	private final Map<OperatorDestOrigin,Map<Set<InetSocketAddress>,Integer>> receivedTupleCounts = new HashMap<OperatorDestOrigin,Map<Set<InetSocketAddress>,Integer>>();

	void process(QueryExecutionMessage qem) {
		if (qem.getQueryId() != queryId) {
			throw new IllegalArgumentException("Message query ID (" + qem.getQueryId() + ") does not match expected query id (" + queryId + ")");
		}
		if (this.isDistributed) {
			if (! qem.distributedDest()) {
				throw new IllegalArgumentException("Message is for a distributed execution but this execution is not distributed");
			}
		} else if (this.namedExec != null) {
			if (! namedExec.equals(qem.namedDest())) {
				throw new IllegalArgumentException("Message is for " + namedExec + " but this execution is not");
			}
		} else {
			if (! qem.centralDest()) {
				throw new IllegalArgumentException("Message is for a central execution but this execution is not centralized");
			}
		}
		try {
			QpMessage msg = (QpMessage) qem;
			final InetSocketAddress origin = msg.getOrigin();
			if (qem instanceof ScanTuplesMessage) {
				ScanTuplesMessage stm = (ScanTuplesMessage) qem;
				Operator<M> o = operators.get(stm.operatorId);
				if (o == null) {
					logger.error("Don't have operator " + stm.operatorId + " for query " + stm.getQueryId());
					app.sendMessage(new ReplyFailure(stm, "Don't have operator " + stm.operatorId + " for query " + stm.getQueryId(), false));
					return;
				}
				KeyReceiver kr = (KeyReceiver) o;
				kr.receiveKeys(stm.phaseNo, stm.getEpochNum(), stm.keyRanges, stm);
				app.sendReplySuccess(stm);
				if (logger.isDebugEnabled()) {
					logger.debug("Processed ScanTuplesMessage for relation " + kr.getRelationId() + " page " + stm.getEpochNum());
				}
			} else if (failedNodes.contains(origin)) {
				return;
			} else if (qem instanceof ShippedTuplesMessage) {
				ShippedTuplesMessage m = (ShippedTuplesMessage) qem;
				QpTupleBag<M> tuples = m.getTuples(this,app.mdf);
				if (logger.isDebugEnabled() && this.recoveryEnabled()) {
					Iterator<QpTuple<M>> it = tuples.recyclingIterator();
					Map<Set<InetSocketAddress>,Integer> currCounts = new HashMap<Set<InetSocketAddress>,Integer>();
					while (it.hasNext()) {
						QpTuple<M> t = it.next();
						Set<InetSocketAddress> contributingNodes = new HashSet<InetSocketAddress>();
						t.addContributingNodesTo(contributingNodes);
						Integer count = currCounts.get(contributingNodes);
						if (count == null) {
							count = 0;
						}
						++count;
						currCounts.put(contributingNodes, count);
					}
					synchronized (receivedTupleCounts) {
						OperatorDestOrigin odo = new OperatorDestOrigin(m.destOperator, m.whichChild, m.getOrigin());
						Map<Set<InetSocketAddress>,Integer> counts = receivedTupleCounts.get(odo);
						if (counts == null) {
							receivedTupleCounts.put(odo, currCounts);
						} else {
							for (Map.Entry<Set<InetSocketAddress>, Integer> me : currCounts.entrySet()) {
								Integer count = counts.get(me.getKey());
								if (count == null) {
									count = 0;
								}
								count += me.getValue();
								counts.put(me.getKey(), count);
							}
						}
					}
				}
				if (bufferedTuples != null) {
					bufferedTuples.get(OperatorAndDest.hashCode(m.destOperator, m.whichChild)).receive(m);
				} else {
					operators.get(m.destOperator).receiveTuples(m.whichChild, tuples);
					recordShipMessageProcessed(m.getOrigin(), m.messageId);
				}
				app.sendReplySuccess(m);
				if (logger.isTraceEnabled()) {
					logger.trace("Node " + this.app.localAddr + " received ShippedTuplesMessage " + m.messageId + " from " + m.getOrigin() + " of length " + tuples.length() + " bytes");
				}
			} else if (qem instanceof EndOfStreamMessage) {
				EndOfStreamMessage m = (EndOfStreamMessage) qem;
				boolean send = false;
				if (logger.isDebugEnabled()) {
					logger.debug("Node " + this.app.localAddr + " received EOS " + m.messageId + " from " + m.getOrigin() + " for operator " + m.destOperator  + " dest " + m.destInput + " phase " + m.phaseNo + " and msg IDs " + Arrays.toString(m.msgIds));
				}
				synchronized (processedShippedMessages) {
					final OperatorDestPhase odp = new OperatorDestPhase(m.destOperator, m.destInput, m.phaseNo);

					LongSet received = processedShippedMessages.get(origin);
					if ((received == null && m.msgIds.length == 0) || (received != null && received.containsAll(m.msgIds))) {
						Set<InetSocketAddress> finishedForODP = finishedStreams.get(odp);
						if (finishedForODP == null) {
							finishedForODP = new HashSet<InetSocketAddress>();
							finishedStreams.put(odp, finishedForODP);
						}
						if (logger.isDebugEnabled()) {
							logger.debug("Node " + this.app.localAddr + " processed EOS for operator " + m.destOperator  + " dest " + m.destInput + " phase " + m.phaseNo + " from " + m.getOrigin());
						}
						if (finishedForODP.add(origin)) {
							Set<InetSocketAddress> senders = this.getSenders(odp); 
							if (senders != null && finishedForODP.equals(senders)) {
								send = true;
							}
						}
					} else {
						List<FinishedStream> finished = pendingFinishedStreams.get(origin);
						if (finished == null) {
							finished = new ArrayList<FinishedStream>();
							pendingFinishedStreams.put(origin, finished);
						}
						LongSet remainingIds = new LongSet(m.msgIds);
						if (received != null) {
							remainingIds.removeAll(received);
						}
						finished.add(new FinishedStream(m, remainingIds));
					}
				}
				if (send) {
					sendEndOfStream(m.destOperator, m.destInput, m.phaseNo);
				}
				app.sendReplySuccess(m);
				recordShipMessageProcessed(m.getOrigin(), m.messageId);
				if (bufferedTuples != null) {
					bufferedTuples.get(OperatorAndDest.hashCode(m.destOperator, m.destInput)).receive(m);
				}
			} else if (qem instanceof PushBufferMessage) {
				PushBufferMessage m = (PushBufferMessage) qem;
				bufferedTuples.get(OperatorAndDest.hashCode(m.operatorId, m.whichInput)).deliver(false);
			} else {
				logger.error("QueryExecution does not know what to do with message " + qem);
			}
		} catch (Exception e) {
			rt.reportException(e);
		}
		rt.activityHasOccurred();
	}

	public InetSocketAddress getNamedNode(String nodeName) {
		return app.getNamedNodehandle(nodeName);
	}

	boolean hasOperator(Integer operator) {
		return operators.containsKey(operator);
	}

	public InetSocketAddress getNodeAddress() {
		return app.localAddr;
	}

	public InetSocketAddress getOwnerAddress() {
		return this.owner;
	}

	public Set<InetSocketAddress> getFailedNodes() {
		synchronized (failedNodes) {
			if (failedNodes.isEmpty()) {
				return Collections.emptySet();
			} else {
				return Collections.unmodifiableSet(failedNodes);
			}
		}
	}

	void beginNewQueryPhase(BeginNewQueryPhase bnqp) throws InterruptedException {
		if (bnqp.queryId != queryId) {
			throw new IllegalArgumentException("Incorrect query ID");
		}
		if (recoveryOperators == null) {
			try {
				app.sendMessage(new ReplyFailure(bnqp, "Query " + bnqp.queryId + " does not support recovery",false));
				logger.error("Query " + bnqp.queryId + " does not support recovery");
			} catch (Exception e) {
				logger.error(e);
			}
			return;
		}
		logger.info(this.toString() + " at " + app.localAddr + " is beginning query phase " + bnqp.phaseNo);
		List<OperatorDestPhase> toSendFinished = new ArrayList<OperatorDestPhase>();
		synchronized (processedShippedMessages) {
			addRouter(bnqp.phaseNo, bnqp.router);
			for (Map.Entry<OperatorDestPhase,Set<InetSocketAddress>> me : finishedStreams.entrySet()) {
				final OperatorDestPhase odp = me.getKey();
				if (odp.phase != bnqp.phaseNo) {
					continue;
				}
				if (me.getValue().equals(this.getSenders(odp))) {
					toSendFinished.add(odp);
				}
			}
		}
		for (OperatorDestPhase odp : toSendFinished) {
			sendEndOfStream(odp.operatorId, odp.dest, odp.phase);
		}

		failedNodes.addAll(bnqp.newlyFailedNodes);
		InetSocketAddress[] newlyFailedNodesArray = new InetSocketAddress[bnqp.newlyFailedNodes.size()];
		newlyFailedNodesArray = bnqp.newlyFailedNodes.toArray(newlyFailedNodesArray);

		final IdRangeSet takenOverByThisNode = bnqp.router.getOwnedRanges(app.localAddr);
		takenOverByThisNode.intersect(bnqp.previousPhaseFailedRanges);

		for (Map.Entry<Integer,Operator<M>> me : operators.entrySet()) {
			Operator<M> o = me.getValue();
			o.beginNewPhase(bnqp.phaseNo, newlyFailedNodesArray, bnqp.previousPhaseFailedRanges.clone());
			if (o instanceof KeyReceiver) {
				((KeyReceiver) o).beginPhase(bnqp.phaseNo, takenOverByThisNode.clone());
			}
		}

		startScans(bnqp.phaseNo);
		rt.activityHasOccurred();
	}

	private void addRouter(int phaseNo, Router router) {
		if (router == null) {
			throw new NullPointerException();
		}
		synchronized (routers) {
			while (routers.size() <= phaseNo) {
				routers.add(null);
			}
			routers.set(phaseNo, router);
		}
	}

	Router getRouter(int phaseNo) {
		synchronized (routers) {
			if (routers.size() <= phaseNo) {
				return null;
			}
			return routers.get(phaseNo);
		}
	}

	Router getMostRecentRouter() {
		synchronized (routers) {
			return routers.get(routers.size() - 1);
		}
	}

	public IdRangeSet getInitiallyOwnedRange() {
		return routers.get(0).getOwnedRanges(app.localAddr);
	}

	Router[] getRouters() {
		synchronized (routers) {
			Router[] retval = new Router[routers.size()];
			return routers.toArray(retval);
		}
	}

	SpoolOperator<M> getSpoolOperator() {
		return spoolOperator;
	}

	public boolean recoveryEnabled() {
		return recoveryOperators != null;
	}

	public Map<Integer,int[]> getRecoveryOperators() {
		return recoveryOperators;
	}

	public boolean discardResults() {
		return config.discardResults;
	}
}