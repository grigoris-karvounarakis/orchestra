package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.p2pqp.DHTService.DHTException;
import edu.upenn.cis.orchestra.p2pqp.QpApplication.RecoveryMode;
import edu.upenn.cis.orchestra.p2pqp.messages.BeginNewQueryPhase;
import edu.upenn.cis.orchestra.p2pqp.messages.QueryOwnerMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.RecordTuplesMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyFailure;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySuccess;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.SpoolNode;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan.RelationAndFilter;

public class QueryOwner<M> implements RecordTuples {
	final QpApplication<M> app;
	final int queryId;
	private final QueryExecution<M> qe;
	private final Logger logger = Logger.getLogger(this.getClass());


	public final int defaultShowDelayMs = 5000;
	public final int defaultClockTickMs = 500;

	private final int epoch;
	private final List<RelationAndFilter> distributedScans;

	private final Map<Integer,IdRangeSet> toBeScanned;
	private final Map<Integer,QpSchema> scanSchemas;

	private int currentPhase = 0;
	private Router currentRouter;

	private final Set<InetSocketAddress> reportedFailedNodes = new HashSet<InetSocketAddress>();


	private final QpApplication.Configuration config;

	private final Map<InetSocketAddress,List<Exception>> exceptions = new HashMap<InetSocketAddress,List<Exception>>();

	private final Map<Integer,QueryPlan<M>> operatorsMap;


	private final List<Thread> startedThreads = new ArrayList<Thread>();


	public static long timeout = 30000;

	private volatile long lastActivity = Long.MAX_VALUE;
	private volatile long currTime;

	private final List<Map<Integer,Set<InetSocketAddress>>> remainingNodesForOperators;
	private final List<Set<InetSocketAddress>> pendingNodesForQueryPhases;

	private final QueryPlan<M> qp;
	private final List<QpSchema> querySchemas;

	QueryOwner(Router queryRouter, int queryId, QpApplication<M> app, int epoch, QueryPlan<M> qp, final Collection<QpSchema> baseSchemas, final Collection<QpSchema> querySchemas, Set<InetSocketAddress> participants, QpApplication.Configuration config) throws Operator.OperatorCreationException, InterruptedException, IOException, DHTException {
		this.queryId = queryId;
		this.qp = qp;
		this.querySchemas = new ArrayList<QpSchema>(querySchemas);
		currentRouter = queryRouter;

		ArrayList<QpSchema> schemas = new ArrayList<QpSchema>(querySchemas.size() + baseSchemas.size());
		schemas.addAll(querySchemas);
		schemas.addAll(baseSchemas);

		if (! (qp instanceof SpoolNode)) {
			throw new IllegalArgumentException("Root of query plan must be a SpoolNode");
		}

		// This base and query schemas
		QpSchema.Source ss = new QpSchema.CollectionSource(schemas);

		this.app = app;

		this.epoch = epoch;
		this.config = config;

		operatorsMap = new HashMap<Integer,QueryPlan<M>>();
		qp.getOperators(operatorsMap);

		pendingNodesForQueryPhases = new ArrayList<Set<InetSocketAddress>>();
		final Set<InetSocketAddress> noNodes = Collections.emptySet();
		pendingNodesForQueryPhases.add(noNodes);

		List<RelationAndFilter> ourDistScans = new ArrayList<RelationAndFilter>();
		qp.getDistributedScans(ourDistScans, ss);
		distributedScans = Collections.unmodifiableList(ourDistScans);
		remainingNodesForOperators = new ArrayList<Map<Integer,Set<InetSocketAddress>>>(operatorsMap.size());

		Map<String,Integer> relCards = new HashMap<String,Integer>();
		for (RelationAndFilter raf : ourDistScans) {
			relCards.put(raf.relation, app.dht.getCardinality(ss.getSchema(raf.relation).relId, epoch));
		}

		Map<Integer,int[]> scanKeyCols = new HashMap<Integer,int[]>(distributedScans.size());
		Map<Integer,Integer> scanCards = new HashMap<Integer,Integer>(distributedScans.size());
		scanSchemas = new HashMap<Integer,QpSchema>();
		toBeScanned = new HashMap<Integer,IdRangeSet>(scanKeyCols.size());

		for (RelationAndFilter raf : distributedScans) {
			QpSchema schema = ss.getSchema(raf.relation);
			scanKeyCols.put(raf.operatorId, schema.getKeyColsListNoCopy());
			scanCards.put(raf.operatorId, relCards.get(raf.relation));
			scanSchemas.put(raf.operatorId, schema);
			toBeScanned.put(raf.operatorId, IdRangeSet.full());
		}

		currTime = System.currentTimeMillis();
		if (logger.isInfoEnabled()) {
			Thread checkThread = new CheckThread(defaultClockTickMs, defaultShowDelayMs);
			checkThread.start();
			startedThreads.add(checkThread);
		}

		qe = new QueryExecution<M>(queryRouter, epoch, queryId, schemas, app, this, qp, config);
		qp.createCentralExecution(qe);
		initRemainingNodesForOperators(0, queryRouter.getParticipants());
	}

	private void initRemainingNodesForOperators(int phaseNo, Set<InetSocketAddress> allNodes) {
		while (remainingNodesForOperators.size() <= phaseNo) {
			remainingNodesForOperators.add(new HashMap<Integer,Set<InetSocketAddress>>());
		}
		remainingNodesForOperators.get(phaseNo).clear();
		for (QueryPlan<M> qp : operatorsMap.values()) {
			Set<InetSocketAddress> remainingNodes;
			if (qp.loc.isCentralized()) {
				remainingNodes = new HashSet<InetSocketAddress>(1);
				remainingNodes.add(app.localAddr);
			} else if (qp.loc.isDistributed()) {
				remainingNodes = new HashSet<InetSocketAddress>(allNodes);
			} else {
				remainingNodes = new HashSet<InetSocketAddress>(1);
				remainingNodes.add(app.getNamedNodehandle(qp.loc.getName()));
			}
			remainingNodesForOperators.get(phaseNo).put(qp.operatorId, remainingNodes);
		}
	}

	void process(QueryOwnerMessage m) throws IOException, InterruptedException {
		if (m.getQueryId() != queryId) {
			throw new IllegalArgumentException("Message query ID (" + m.getQueryId() + ") does not match expected query id (" + queryId + ")");
		}

		if (m instanceof RecordTuplesMessage) {
			final RecordTuplesMessage rtm = (RecordTuplesMessage) m;
			synchronized (this) {
				final InetSocketAddress origin = rtm.getOrigin();
				if (reportedFailedNodes.contains(origin)) {
					return;
				}
				for (Map.Entry<Integer,IdRangeSet> me : rtm.scanned.entrySet()) {
					keysScanned(me.getKey(), me.getValue(), origin);
				}
				Map<OperatorAndPhase,? extends Collection<QpTupleKey>> missing = rtm.getMissingKeys(qe);
				for (Map.Entry<OperatorAndPhase, ? extends Collection<QpTupleKey>> me : missing.entrySet()) {
					OperatorAndPhase oap = me.getKey();
					keysNotFound(oap.operator, me.getValue(), oap.phase, origin);
				}
				for (Exception e : rtm.exceptions) {
					reportException(origin, e);
				}
				if (! rtm.failed.isEmpty()) {
					this.nodesHaveFailed(rtm.failed, origin);
				}
				for (OperatorAndPhase oap : rtm.finished) {
					this.operatorHasFinished(oap.operator, oap.phase, origin);
				}
				messageReceived(origin, rtm.messageId);
			}
			app.sendReplySuccess(rtm);
		} else {
			logger.error("Don't know what to do with QueryOwnerMessage " + m);
			app.sendMessage(new ReplyFailure((QpMessage) m, "Unexpected type of QueryOwnerMessage", false));
		}
	}

	void start() throws DHTException, InterruptedException {
		initDistributedScans(0, IdRangeSet.full(), qe.getRouter(0));
		qe.start();
	}

	void continueQueryOwner(int currentEpoch) throws InterruptedException, DHTException {
		qe.continueEpoch(currentEpoch);
	}

	QueryExecution<M> getOwnedExecution() {
		return qe;
	}

	private void attemptRecovery(Set<InetSocketAddress> newlyFailedNodes) throws DHTException, InterruptedException, IOException {
		final Router newRouter;
		final int thisPhase;
		final IdRangeSet failedRanges;
		synchronized (this) {
			thisPhase = ++currentPhase;

			Router oldRouter = getOwnedExecution().getRouter(thisPhase - 1);
			failedRanges = IdRangeSet.empty();
			for (InetSocketAddress failedNode : newlyFailedNodes) {
				failedRanges.add(oldRouter.getOwnedRanges(failedNode));
			}
			newRouter = config.createSpecialRecoveryRouter ? oldRouter.createRecoveryRouter(newlyFailedNodes) : oldRouter.getRouterWithout(newlyFailedNodes);
			currentRouter = newRouter;
			logger.warn("Beginning recovery for " + newlyFailedNodes + " as phase " + thisPhase + ", new router:\n" + newRouter);

			for (RelationAndFilter raf : distributedScans) {
				IdRangeSet remaining = toBeScanned.get(raf.operatorId);
				if (remaining == null) {
					remaining = IdRangeSet.empty();
					toBeScanned.put(raf.operatorId, remaining);
				}
				for (IdRange range : failedRanges) {
					remaining.add(range);
				}
			}


			initRemainingNodesForOperators(thisPhase, newRouter.getParticipants());

			final Set<InetSocketAddress> noNodes = Collections.emptySet();
			while (this.pendingNodesForQueryPhases.size() <= thisPhase) {
				pendingNodesForQueryPhases.add(noNodes);
			}
			pendingNodesForQueryPhases.set(thisPhase, new HashSet<InetSocketAddress>(newRouter.getParticipants()));
		}

		for (final InetSocketAddress node : newRouter.getParticipants()) {
			app.sendMessageAwaitReply(new BeginNewQueryPhase(node,queryId,thisPhase,newRouter,failedRanges,newlyFailedNodes),
					new ReplyContinuation() {
				private boolean finished = false;

				public synchronized boolean isFinished() {
					return finished;
				}

				public synchronized void processReply(QpMessage m) {
					finished = true;
					if (! (m instanceof ReplySuccess)) {
						nodesHaveFailed(Collections.singleton(node));
						return;
					}
					synchronized (QueryOwner.this) {
						pendingNodesForQueryPhases.get(thisPhase).remove(node);
					}
				}
			}, ReplySuccess.class);
		}

		initDistributedScans(thisPhase, failedRanges, newRouter);
		qe.beginNewQueryPhase(new BeginNewQueryPhase(app.localAddr,queryId,thisPhase,newRouter,failedRanges,newlyFailedNodes));
	}

	SpoolOperator<M> getSpoolOperator() {
		return qe.getSpoolOperator();
	}

	private class CheckThread extends Thread {
		private final long showIntervalMs;
		private final long sleepMs;
		private long lastCheck = System.currentTimeMillis();
		private long lastShow = lastCheck;

		CheckThread(int timerResolutionMs, int showIntervalMs) {
			super(app.tg, "CheckThread(" + queryId + ")");
			this.showIntervalMs = showIntervalMs;

			long min, max;
			if (timerResolutionMs > showIntervalMs) {
				max = timerResolutionMs;
				min = showIntervalMs;
			} else {
				min = timerResolutionMs;
				max = showIntervalMs;				
			}

			if (min * 2 > max) {
				sleepMs = max / 4;
			} else {
				sleepMs = min;
			}
		}

		public void run() {
			long currTime = System.currentTimeMillis();
			lastActivity = currTime;
			while (! isInterrupted()) {
				try {
					sleep(sleepMs);
				} catch (InterruptedException ie) {
					return;
				}

				currTime = System.currentTimeMillis();
				long lastActivity = QueryOwner.this.lastActivity;
				QueryOwner.this.currTime = currTime;

				currTime = System.currentTimeMillis();
				QueryOwner.this.currTime = currTime;
				lastActivity = QueryOwner.this.lastActivity;

				if (currTime - lastActivity > timeout) {
					boolean isThrottled = app.throttlingOccurring();
					if (isThrottled) {
						lastActivity = currTime;
					} else {
						reportException(new RuntimeException("Query owner has timed out"));
					}
				}

				if (currTime - lastShow > showIntervalMs) {
					showStatus();
					lastShow = currTime;
				}
			}
		}
	}



	public void waitUntilDone() throws InterruptedException, QueryFailure {
		getSpoolOperator().waitUntilFinished();

		synchronized (this) {
			showStatus();
			for (Map.Entry<InetSocketAddress, List<Exception>> me : exceptions.entrySet()) {
				InetSocketAddress node = me.getKey();
				List<Exception> exs = me.getValue();
				throw new QueryFailure(node, exs.get(0));
			}
		}
	}


	@Override
	public void keysScanned(int operatorId, IdRange range) {
		keysScanned(operatorId,Collections.singletonList(range),app.localAddr);
	}

	private synchronized void keysScanned(int operatorId, Iterable<IdRange> ranges, InetSocketAddress nodeId) {
		if (reportedFailedNodes.contains(nodeId)) {
			return;
		}
		IdRangeSet remaining = toBeScanned.get(operatorId);
		if (remaining != null) {
			for (IdRange range : ranges) {
				remaining.remove(range);
			}
			if (remaining.isEmpty()) {
				toBeScanned.remove(operatorId);
			}
			if (logger.isDebugEnabled()) {
				logger.debug("Operator " + operatorId + " at " + nodeId + " scanned " + ranges + ", remaining for operator: " + remaining);
			}
		}
		lastActivity = currTime;
	}


	private synchronized void reportException(InetSocketAddress node, Exception e) {
		List<Exception> exs = exceptions.get(node);
		if (exs == null) {
			exs = new ArrayList<Exception>();
			exceptions.put(node, exs);
		}
		exs.add(e);

		getSpoolOperator().failed();
		logger.error("Received exception from node " + node, e);
	}

	@Override
	public void reportException(Exception e) {
		reportException(app.localAddr,e);
	}

	@Override
	public void keysNotFound(int operatorId, Collection<QpTupleKey> keys, int phaseNo) {
		keysNotFound(operatorId, keys, phaseNo, app.localAddr);
	}

	private void keysNotFound(int operatorId, Collection<QpTupleKey> keys, int phaseNo, InetSocketAddress nodeId) {
		if (keys.isEmpty()) {
			return;
		}
		// TODO: do something cleverer here
		reportException(new RuntimeException("Missing tuples for " + operatorId + " at node " + nodeId + " phase " + phaseNo + ": " + keys));		
	}

	public void operatorHasFinished(int operator, int phase) {
		operatorHasFinished(operator,phase,app.localAddr);
	}

	private synchronized void operatorHasFinished(int operator, int phase, InetSocketAddress nodeId) {
		Set<InetSocketAddress> remainingNodes = this.remainingNodesForOperators.get(phase).get(operator);
		if (remainingNodes == null) {
			logger.warn("No remaining nodes for operator " + operator);
			return;
		}
		remainingNodes.remove(nodeId);
		if (remainingNodes.isEmpty()) {
			this.remainingNodesForOperators.get(phase).remove(operator);
		}
		if (logger.isDebugEnabled()) {
			logger.debug("Operator " + operator + " at " + nodeId + " in phase " + phase + " recorded finished at QueryOwner(" + queryId + ")");
		}
	}

	private static final NumberFormat nf = NumberFormat.getPercentInstance();
	static {
		nf.setMaximumFractionDigits(2);
	}

	private void showStatus() {
		if (! logger.isInfoEnabled()) {
			return;
		}
		synchronized (this) {
			StringBuilder sb = new StringBuilder("QueryOwner for query " + queryId + " is waiting for scans: ");
			StringBuilder sb2 = new StringBuilder("QueryOwner for query " + queryId + " is waiting for scans: ");
			List<Integer> remainingScans = new ArrayList<Integer>(toBeScanned.keySet());
			Collections.sort(remainingScans);
			Set<InetSocketAddress> nodes = currentRouter.getParticipants();
			for (Integer op : remainingScans) {
				double remainingPercent = toBeScanned.get(op).remainingFrac();
				sb.append("\n\t#" + op + ": " + nf.format(remainingPercent));
				sb2.append("\n\t#" + op + ": " + nf.format(remainingPercent));
				for (InetSocketAddress node : nodes) {
					boolean found = false;
					double amount = 0.0;
					final IdRangeSet ownedRangesSet = currentRouter.getOwnedRanges(node);
					final IdRange[] ownedRanges = ownedRangesSet.toArray();
					IdRangeSet remainingAtNode = IdRangeSet.empty();
					for (IdRange range : toBeScanned.get(op)) {
						for (IdRange owned : ownedRanges) {
							IdRange intersection = range.intersect(owned);
							if (! intersection.isEmpty()) {
								amount += intersection.getSize();
								remainingAtNode.add(intersection);
								found = true;
							}
						}
					}
					if (found) {
						sb.append("\n\t\t" + node + ": " + nf.format(amount / ownedRangesSet.getSize()));
						if (logger.isDebugEnabled()) {
							sb2.append("\n\t\t" + node + ": " + nf.format(amount / ownedRangesSet.getSize()) + " " + remainingAtNode);
						}
					}
				}
			}
			if (! remainingScans.isEmpty()) {
				logger.info(sb);
				logger.debug(sb2);
			}


			sb.setLength(0);
			sb2.setLength(0);
			sb.append("QueryOwner for query " + queryId + " is still starting query phase at nodes: ");
			boolean startingPhase = false;
			int phase = 0;
			for (Set<InetSocketAddress> remaining : pendingNodesForQueryPhases) {
				if (! remaining.isEmpty()) {
					startingPhase = true;
					sb.append("\n\t#" + phase + ": " + remaining);				
				}
				++phase;
			}

			if (startingPhase) {
				logger.info(sb);
			}
			sb.setLength(0);
			sb.append("QueryOwner for query " + queryId + " is waiting for the following operators to complete: ");
			List<Integer> remainingOperators = new ArrayList<Integer>(this.remainingNodesForOperators.get(currentPhase).keySet());
			Collections.sort(remainingOperators);
			for (Integer op : remainingOperators) {
				Set<InetSocketAddress> remainingNodes = remainingNodesForOperators.get(currentPhase).get(op);
				sb.append("\n\t#" + op + ": " + (remainingNodes.size() > 4 ? Integer.toString(remainingNodes.size()) : remainingNodes.toString()));
			}
			logger.info(sb);
		}
	}

	synchronized public long closeOwner() {
		long sentCount = 0;
		try {
			sentCount = qe.close();
			for (Thread t : startedThreads) {
				t.interrupt();
				t.join();
			}
		} catch (InterruptedException ie) {
		} finally {
			startedThreads.clear();
			toBeScanned.clear();
			logger.info("Closed QueryOwner for query " + queryId);
		}
		return sentCount;
	}

	public void close() {
		// Only for compatibility with RecordTuples
	}
	
	private Map<InetSocketAddress,List<Long>> msgsReceived = Collections.synchronizedMap(new HashMap<InetSocketAddress,List<Long>>());

	public void messageReceived(InetSocketAddress from, long msgId) {
		List<Long> received = msgsReceived.get(from);
		if (received == null) {
			received = new ArrayList<Long>();
			msgsReceived.put(from, received);
		}
		received.add(msgId);
		lastActivity = currTime;
	}

	public Map<InetSocketAddress,Integer> getMessagesReceived() {
		Map<InetSocketAddress,Integer> counts = new HashMap<InetSocketAddress,Integer>(msgsReceived.size());
		for (Map.Entry<InetSocketAddress,List<Long>> me : msgsReceived.entrySet()) {
			counts.put(me.getKey(), me.getValue().size());
		}
		return counts;
	}

	public void activityHasOccurred () {
		lastActivity = currTime;
	}

	private void initDistributedScans(final int phaseNo, final IdRangeSet rangesToSend, final Router phaseRouter) throws DHTException, InterruptedException {
		for (final RelationAndFilter raf : distributedScans) {
			int relId = getOwnedExecution().getSchema(raf.relation).relId;
			app.dht.sendKeyTuples(phaseRouter, relId, epoch, queryId, raf.operatorId, raf.keyFilter, this, rangesToSend, phaseNo, this.qe.config.compressionLevel);
		}
	}

	private boolean restarted = false;

	synchronized private void nodesHaveFailed(Collection<InetSocketAddress> nodes, InetSocketAddress accordingTo) {
		if (! getOwnedExecution().getRouter(0).getParticipants().containsAll(nodes)) {
			throw new IllegalArgumentException("Nodes " + nodes + " are not all participating in query");
		}
		if (config.recoveryMode == RecoveryMode.INCREMENTAL) {
			Set<InetSocketAddress> newlyFailedNodes = new HashSet<InetSocketAddress>();
			Set<InetSocketAddress> allFailedNodes = new HashSet<InetSocketAddress>();
			synchronized (this) {
				for (InetSocketAddress node : nodes) {
					if (reportedFailedNodes.add(node)) {
						newlyFailedNodes.add(node);
					}
				}
				allFailedNodes.addAll(nodes);
			}
			app.nodesHaveFailed(allFailedNodes);

			if (newlyFailedNodes.isEmpty()) {
				return;
			}
			try {
				attemptRecovery(newlyFailedNodes);
			} catch (Exception e) {
				logger.fatal("Error reporting node failure", e);
			}
		} else {
			synchronized (this) {
				reportedFailedNodes.addAll(nodes);
				app.nodesHaveFailed(reportedFailedNodes);
				if (config.recoveryMode == RecoveryMode.RESTART && (! restarted)) {
					restarted = true;
					try {
						Router r = config.createSpecialRecoveryRouter ? qe.getRouter(0).createRecoveryRouter(reportedFailedNodes) : qe.getRouter(0).getRouterWithout(reportedFailedNodes);
						logger.info("Restarting query " + queryId + " as " + queryId + 100000 + " using router:\n" + r);
						app.beginQuery(queryId + 100000, epoch, qp, querySchemas, qe.config, queryId, r);
						// Do this here so we don't redirect if there's an error restarting the query
						app.restartedQueries.put(queryId, queryId + 100000);
					} catch (Exception e) {
						logger.error("Error restarting query", e);
					}
				}
				reportException(new RuntimeException("Node(s) " + nodes + " have failed, according to " + accordingTo));
			}
		}
	}

	public synchronized void nodesHaveFailed(Collection<InetSocketAddress> nodes) {
		Set<InetSocketAddress> failed = new HashSet<InetSocketAddress>(nodes);
		failed.retainAll(getOwnedExecution().getRouter(0).getParticipants());
		if (failed.isEmpty()) {
			return;
		}
		logger.warn("Query owner reported that nodes " + failed + " failed for query " + queryId, new Throwable());
		nodesHaveFailed(failed,app.localAddr);
	}

	synchronized int getCurrentPhase() {
		return currentPhase;
	}

	synchronized Set<InetSocketAddress> getReportedFailedNodes() {
		return new HashSet<InetSocketAddress>(reportedFailedNodes);
	}
}
