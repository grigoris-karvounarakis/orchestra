package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.io.PrintStream;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.zip.Deflater;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;

import edu.upenn.cis.orchestra.p2pqp.DHTService.DHTException;
import edu.upenn.cis.orchestra.p2pqp.DHTService.KeyTupleSink;
import edu.upenn.cis.orchestra.p2pqp.MsgStatusCache.MsgStatus;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.p2pqp.Router.NodeInfo;
import edu.upenn.cis.orchestra.p2pqp.TupleStore.TupleStoreException;
import edu.upenn.cis.orchestra.p2pqp.messages.BeginNewQueryPhase;
import edu.upenn.cis.orchestra.p2pqp.messages.CheckRelation;
import edu.upenn.cis.orchestra.p2pqp.messages.ConnectMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.DHTMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.DistributeQueryContinueMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.DistributeQueryMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.DoesNotHaveQuery;
import edu.upenn.cis.orchestra.p2pqp.messages.DummyMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.GarbageCollect;
import edu.upenn.cis.orchestra.p2pqp.messages.GetNodeInfo;
import edu.upenn.cis.orchestra.p2pqp.messages.LocalRelationIs;
import edu.upenn.cis.orchestra.p2pqp.messages.MessageDestHasDied;
import edu.upenn.cis.orchestra.p2pqp.messages.MissingTuplesAre;
import edu.upenn.cis.orchestra.p2pqp.messages.NodeInfoIs;
import edu.upenn.cis.orchestra.p2pqp.messages.QueryExecutionMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.QueryOwnerMessage;
import edu.upenn.cis.orchestra.p2pqp.messages.QueryTornDown;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyException;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyFailure;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySendingFailed;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplySuccess;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyTimeout;
import edu.upenn.cis.orchestra.p2pqp.messages.SendKnownNodes;
import edu.upenn.cis.orchestra.p2pqp.messages.TearDownQueryMessage;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlan;
import edu.upenn.cis.orchestra.p2pqp.plan.QueryPlanWithSchemas;
import edu.upenn.cis.orchestra.util.DomUtils;
import edu.upenn.cis.orchestra.util.Holder;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.LongList;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.Pair;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class QpApplication<M> implements SocketManagerClient {
	// Number of times to retry an operation that fails
	static final int DEFAULT_NUM_RETRIES = 5;
	// Delay between failure and retry, in milliseconds
	static final int DEFAULT_RETRY_DELAY = 1000;

	// Use QuickLZ by default
	static final int DEFAULT_COMPRESSION_LEVEL = -1;

	// Time to wait for query dissemination before timing out
	public static int QUERY_DISSEMINATION_TIME_MS = 45000;

	// Maximum amount of time to wait between responses to a flood before giving up
	public static int FLOODING_MAX_WAIT_INTERVAL_MS = 5000;

	// Time to wait after receiving notification that a node joined or left before updating
	// the known nodes set
	public static final int NODE_NOTIFICATION_WAIT_MS = 2000;


	// Time to retain information about a received message, in milliseconds
	static final int MSG_RETAIN_MS = 600000;

	// Maximum size of a message, 128 KB
	public static final int MAX_MSG_SIZE = 128 * 1024;

	final Logger logger = Logger.getLogger(this.getClass());

	final public static String instanceName = "Orchestra QP";
	final public static String scribeInstanceName = "Orchestra QP Multicasting";
	final public static String allNodesName = "All Nodes";

	public static int numMessageProcessingThreads = 5;
	Id nodeId;

	final ThreadGroup tg;
	private List<MessageProcessingThread> messageProcessingThreads;
	// Table to store reply data, is synchronized
	private Map<Long,ReplyData> replyData;
	// Record of which messages were sent where, is not synchronized
	private Map<InetSocketAddress,Set<Long>> messageDests;

	private final Map<InetSocketAddress,NodeInfoWithLiveness> knownNodes = new HashMap<InetSocketAddress,NodeInfoWithLiveness>();

	TupleStore<M> store;
	DHTService<M> dht;
	private Map<Integer,Integer> firstEpochsForTables;

	private ShowStatusThread showQueueThread;

	// Lock for these is ownedQueries
	private Map<Integer,QueryOwner<M>> ownedQueries;
	private Map<Integer,QueryExecution<M>> distributedQueries;
	private Map<Pair<Integer,String>,QueryExecution<M>> namedNodeQueries;
	private Map<Integer,Collection<QueryExecutionMessage>> pendingExecutionMessages;
	private Set<Integer> finishedQueries;

	private Map<String,Record> messageCounts;

	private Set<String> localNames;
	private Map<String,InetSocketAddress> allNames;

	final MetadataFactory<M> mdf;

	private MsgStatusCache msgStatus;

	private QpMessageSerialization messageSerialization;

	private Timer timer;

	private final ScratchFileGenerator sfg;

	private volatile Router.Type routerType = Router.Type.EVEN;
	private volatile Router router = null;

	final InetSocketAddress localAddr;
	final SocketManager socketManager;
	final int replicationFactor;

	private final Class<M> metadataClass;

	public long totalTupleSize = 0;
	public long totalTupleNum = 0;
	public long totalOperatorState = 0;
	public long totalOutputShippingMessage = 0;
	public long totalInputShippingMessage = 0;

	final Map<Integer,Long> startCountsForQueries = Collections.synchronizedMap(new HashMap<Integer,Long>());

	private final Map<Long,byte[]> pendingMessages = Collections.synchronizedMap(new HashMap<Long,byte[]>());
	
	public QpApplication(InetSocketAddress bindAddr, InetSocketAddress publicAddr, Id nodeId, InetSocketAddress bootNode, ScratchFileGenerator sfg, Environment env, String storeTableName, String indexTableName, TableNameGenerator tng, MetadataFactory<M> mdf, Map<String,InetSocketAddress> allNames) throws IOException, DatabaseException, InterruptedException {
		this(bindAddr,publicAddr,nodeId,bootNode,sfg,env,storeTableName,indexTableName,tng,mdf,allNames,1);
	}

	public QpApplication(InetSocketAddress bindAddr, InetSocketAddress publicAddr, Id nodeId, InetSocketAddress bootNode, ScratchFileGenerator sfg, Environment env, String storeTableName, String indexTableName, TableNameGenerator tng, MetadataFactory<M> mdf, Map<String,InetSocketAddress> allNames, int replicationFactor) throws IOException, DatabaseException, InterruptedException {
		if (publicAddr == null) {
			publicAddr = bindAddr;
		}
		this.metadataClass = mdf.getMetadataClass();
		if (this.metadataClass == null) {
			throw new NullPointerException("metadataClass cannot be null");
		}
		if (replicationFactor % 2 != 1) {
			throw new IllegalArgumentException("Replication factor must be odd");
		}
		this.replicationFactor = replicationFactor;
		if (mdf instanceof NullMetadataFactory) {
			this.mdf = null;
		} else {
			this.mdf = mdf;
		}
		if (nodeId == null) {
			// Hash IP address and port to get DHT Id
			ScratchOutputBuffer sob = new ScratchOutputBuffer();
			sob.writeInetSocketAddress(publicAddr);
			nodeId = Id.fromContent(sob.getData());
		}
		this.nodeId = nodeId;

		if (bootNode != null && bootNode.equals(publicAddr)) {
			bootNode = null;
		}

		if (allNames != null) {
			this.allNames = new HashMap<String,InetSocketAddress>(allNames);
		} else {
			this.allNames = Collections.emptyMap();
		}
		localNames = Collections.synchronizedSet(new HashSet<String>());
		tg = new ThreadGroup("Orchestra QP " + publicAddr + " Threads");

		messageCounts = new HashMap<String,Record>();

		messageDests = new HashMap<InetSocketAddress,Set<Long>>();

		this.sfg = sfg;
		msgStatus = new MsgStatusCache(MSG_RETAIN_MS);

		this.localAddr = publicAddr; 
		messageSerialization = new QpMessageSerialization();
		socketManager = new SocketManager(messageSerialization, this.sfg, this, tg, bindAddr, publicAddr);

		timer = new Timer(localAddr + " timer");
		timer.schedule(new TimerTask() {

			@Override
			public void run() {
				timer.purge();
			}

		}, 5000, 5000);
		ownedQueries = new HashMap<Integer,QueryOwner<M>>();
		distributedQueries = new HashMap<Integer,QueryExecution<M>>();
		namedNodeQueries = new HashMap<Pair<Integer,String>,QueryExecution<M>>();
		pendingExecutionMessages = new HashMap<Integer,Collection<QueryExecutionMessage>>();
		finishedQueries = new HashSet<Integer>();

		firstEpochsForTables = new HashMap<Integer,Integer>();

		messageProcessingThreads = new ArrayList<MessageProcessingThread>(numMessageProcessingThreads);
		for (int i = 0; i < numMessageProcessingThreads; ++i) {
			MessageProcessingThread mpt = new MessageProcessingThread(i + 1);
			mpt.start();
			messageProcessingThreads.add(mpt);
		}

		replyData = Collections.synchronizedMap(new HashMap<Long,ReplyData>());

		showQueueThread = new ShowStatusThread();
		showQueueThread.start();

		store = new BDbTupleStore<M>(env, storeTableName, mdf);
		dht = new DHTService<M>(this, env, tng, this.sfg, indexTableName);

		List<NodeInfo> nis = Collections.singletonList(new NodeInfo(nodeId, localAddr));
		knownNodes.put(localAddr, new NodeInfoWithLiveness(nodeId, localAddr));
		router = Router.createRouter(nis, replicationFactor, routerType);
		logger.info("Finished creating new node (ID " + nodeId + ", Address " + localAddr);


		if (bootNode != null) {
			this.sendMessage(new ConnectMessage(bootNode, this.nodeId));
		}

		DatabaseConfig dc = new DatabaseConfig();
		dc.setSortedDuplicates(false);
		dc.setTemporary(true);
		dc.setAllowCreate(true);
	}

	private static class Record implements Comparable<Record> {
		String className;
		int count;

		Record(String className) {
			this.className = className;
			count = 1;
		}

		Record(String className, int count) {
			this.className = className;
			this.count = count;
		}

		// Sort in decreasing order by count
		public int compareTo(Record r) {
			return r.count - count;
		}

		void increment() {
			++count;
		}
	}

	private class ShowStatusThread extends Thread {

		ShowStatusThread() {
			super(tg, "Show Status Thread");
		}


		public void run() {
			long lastTime = System.currentTimeMillis();
			while (! isInterrupted()) {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
					return;
				}
				if (logger.isInfoEnabled()) {
					Collection<InetSocketAddress> throttling = socketManager.getThrottledNodes();
					Collection<InetSocketAddress> throttledBy = socketManager.getThrottledByNodes();
					if (! throttling.isEmpty()) {
						logger.info("Node " + localAddr + " is throttling " + throttling);
					}

					if (! throttledBy.isEmpty()) {
						logger.info("Node " + localAddr + " is throttled by " + throttledBy);
					}
					if (logger.isInfoEnabled()) {
						long currTime = System.currentTimeMillis();
						ArrayList<Record> recs;
						synchronized (messageCounts) {
							recs = new ArrayList<Record>(messageCounts.values());
							messageCounts.clear();
						}
						Collections.sort(recs);
						Iterator<Record> it = recs.iterator();
						StringBuilder sb = new StringBuilder("Messages processed at " + localAddr + " in last " + (currTime - lastTime) +" msec: ");
						while (it.hasNext()) {
							Record r = it.next();
							sb.append(r.count + " " + r.className);
							if (it.hasNext()) {
								sb.append(", ");
							}
						}
						logger.info(sb);
						lastTime = currTime;
					}
				}
			}
		}
	}

	public boolean isReady() {
		return router != null;
	}

	public void stop() throws DHTException, TupleStoreException {
		try {
			socketManager.close();
			logger.info("Closed " + localAddr + " socket manager");
		} catch (IOException ioe) {
			logger.error("Error shutting down socket manager", ioe);
		} catch (InterruptedException ie) {
			return;
		}

		if (timer != null) {
			timer.cancel();
			timer = null;
		}
		logger.info("Stopped " + localAddr + " timer");

		try {
			if (msgStatus != null) {
				msgStatus.close();
			}

			if (showQueueThread != null) {
				showQueueThread.interrupt();
				showQueueThread.join();
				showQueueThread = null;
			}

			if (messageProcessingThreads != null) {
				for (int i = 0; i < messageProcessingThreads.size(); ++i) {
					MessageProcessingThread mpt = messageProcessingThreads.get(i);
					mpt.interrupt();
					mpt.join();
				}
				messageProcessingThreads = null;
			}
			logger.info("Stopped " + localAddr + " message processing threads");

			synchronized (ownedQueries) {
				for (QueryOwner<M> qo : ownedQueries.values()) {
					qo.close();
				}
				ownedQueries.clear();
				logger.info("Stopped " + localAddr + " owned queries");

				for (QueryExecution<M> qe : distributedQueries.values()) {
					qe.close();
				}
				distributedQueries.clear();
				logger.info("Stopped " + localAddr + " distributed queries");

				for (QueryExecution<M> qe : namedNodeQueries.values()) {
					qe.close();
				}
				namedNodeQueries.clear();
				logger.info("Stopped " + localAddr + " named node queries");
			}



			if (store != null) {
				store.close();
				store = null;
			}
			logger.info("Stopped " + localAddr + " store");


			if (dht != null) {
				dht.close();
				dht = null;
			}
			logger.info("Stopped " + localAddr + " DHT service");

			this.pendingMessages.clear();
		} catch (Exception e) {
			logger.error("Error stopping " + localAddr, e);
		}
	}

	private final Object updateRoutingTableLock = new Object();
	private UpdateRoutingTable updateRoutingTable;

	public void scheduleRoutingTableUpdate() throws IOException, InterruptedException {
		boolean created = false;
		synchronized (updateRoutingTableLock) {
			if (updateRoutingTable == null && timer != null) {
				created = true;
				updateRoutingTable = new UpdateRoutingTable();
			}
		}
		if (created) {
			getNodeInfo(updateRoutingTable);
		}
	}

	public void performRoutingTableUpdate() throws InterruptedException, IOException {
		boolean created = false;
		UpdateRoutingTable toWaitFor;
		synchronized (updateRoutingTableLock) {
			if (updateRoutingTable == null && timer != null) {
				created = true;
				updateRoutingTable = new UpdateRoutingTable();
			}
			toWaitFor = updateRoutingTable;
		}
		if (created) {
			getNodeInfo(updateRoutingTable);
		}
		synchronized (updateRoutingTableLock) {
			while (! toWaitFor.done) {
				updateRoutingTableLock.wait();
			}
			if (toWaitFor.err != null) {
				throw new RuntimeException("Error updating routing table", toWaitFor.err);
			}
		}
	}

	private class UpdateRoutingTable implements NodeInfoSink {
		private boolean done = false;
		Exception err = null;

		public void deliver(List<NodeInfo> result) {
			List<NodeInfo> toUse;
			List<NodeInfoWithLiveness> toSend;
			synchronized (knownNodes) {
				for (NodeInfo ni : result) {
					NodeInfoWithLiveness niwl = knownNodes.get(ni.qpAddress);
					if (niwl == null) {
						knownNodes.put(ni.qpAddress, new NodeInfoWithLiveness(ni.id, ni.qpAddress));
					} else {
						if (! niwl.id.equals(ni.id)) {
							knownNodes.put(ni.qpAddress, new NodeInfoWithLiveness(ni.id, ni.qpAddress));
							logger.warn("ID for " + niwl.address + " changed from " + niwl.id + " to " + ni.id);
						}
						niwl.alive = true;
					}
				}
				toUse = new ArrayList<NodeInfo>(knownNodes.size());
				for (NodeInfoWithLiveness niwl : knownNodes.values()) {
					if (niwl.alive) {
						toUse.add(niwl.getNodeInfo());
					}
				}
				toSend = new ArrayList<NodeInfoWithLiveness>(knownNodes.values());
			}
			Router r;
			try {
				r = Router.createRouter(toUse, replicationFactor, routerType);
			} catch (Exception e) {
				logger.error("Error constructing routing table for node " + localAddr, e);
				return;
			}
			synchronized (updateRoutingTableLock) {
				if (done) {
					return;
				}
				router = r;
				updateRoutingTable = null;
				done = true;
				updateRoutingTableLock.notifyAll();
			}
			try {
				for (NodeInfo ni : toUse) {
					QpApplication.this.sendMessage(new SendKnownNodes(ni.qpAddress, toSend));
				}
			} catch (IOException ioe) {
				logger.error("Error sending known nodes", ioe);
			} catch (InterruptedException ie) {
				return;
			}
		}

		public void exception(Exception e) {
			logger.info("Error retrieving range info, routing table unchanged", e);
			synchronized (updateRoutingTableLock) {
				updateRoutingTable = null;
				done = true;
				err = e;
				updateRoutingTableLock.notifyAll();
			}
		}
	}

	void processMessage(QpMessage m) {
		try {
			if (m.isReply()) {
				for (long id : m.getOrigIds()) {
					// Dispatch through reply table
					ReplyData rd = replyData.get(id);
					if (rd == null) {
						// Silently ignore replies we're not interested in
						if (logger.isTraceEnabled()) {
							logger.trace("Ignoring reply " + m + " to " + id + " since its reply continuation has finished");
						}
						continue;
					}
					synchronized (rd) {
						// Avoid race condition when two replies arrive simultaneously,
						// and one causes success or failure
						if (rd.hasFinished()) {
							continue;
						}

						rd.cancelPendingAction();

						boolean isSuccess = false;
						for (Class<?> c : rd.successReplies) {
							if (c.isInstance(m)) {
								isSuccess = true;
								break;
							}
						}
						synchronized (messageDests) {
							Set<Long> msgIds = messageDests.get(rd.sentTo);
							if (msgIds != null) {
								msgIds.remove(id);
								if (msgIds.isEmpty()) {
									messageDests.remove(rd.sentTo);
								}
							}
						}
						if ((! rd.retriesLeft()) || isSuccess || (! m.canRetry())) {
							rd.rc.processReply(m);
							if (rd.rc.isFinished() || ((! isSuccess) && ((! rd.retriesLeft()) || (! m.canRetry())))) {
								rd.setFinished();
								if (rd.retryable) {
									replyData.remove(id);
								}
								this.removeSentMessage(rd.msgId);
							} else {
								rd.startPendingAction(this);
							}
						} else {
							rd.recordFailure(m.retryImmediately());
							rd.startPendingAction(this);
						}
					}
				}
				return;
			}
			if (m.getOrigin() != null) {
				MsgStatus ms = msgStatus.getStatus(m.getOrigin(), m.messageId);
				if (ms == MsgStatus.UNKNOWN || ms == MsgStatus.CAN_RETRY) {
					msgStatus.recordMessageReceived(m.getOrigin(), m.messageId);
				} else if (ms == MsgStatus.SUCCEEDED) {
					sendReplySuccess(m);
					return;
				} else if (ms == MsgStatus.FAILED) {
					sendMessage(new ReplyFailure(m, "Message has already failed", false));
					return;
				} else if (ms == MsgStatus.CANNOT_RETRY) {
					sendMessage(new ReplyFailure(m, "Message has been processed and cannot be retried", false));
					return;
				} else if (ms == MsgStatus.RECEIVED) {
					return;
				} else {
					throw new RuntimeException("Don't know how to handle " + ms);
				}
			}
			if (m instanceof DHTMessage) {
				dht.processMessage((DHTMessage) m);
			} else if (m instanceof QueryOwnerMessage) {
				QueryOwnerMessage qom = (QueryOwnerMessage) m;
				QueryOwner<M> qo;
				synchronized (ownedQueries) {
					qo = ownedQueries.get(qom.getQueryId());
				}
				if (qo == null) {
					sendMessage(new DoesNotHaveQuery(m, qom.getQueryId()));
				} else {
					qo.process(qom);
				}
			} else if (m instanceof QueryExecutionMessage) {
				QueryExecutionMessage qem = (QueryExecutionMessage) m;
				QueryExecution<M> qe;
				final int queryId = qem.getQueryId();
				synchronized (ownedQueries) {
					if (qem.distributedDest()) {
						qe = distributedQueries.get(queryId);
					} else if (qem.centralDest()) {
						QueryOwner<M> qo = ownedQueries.get(queryId);
						if (qo == null) {
							qe = null;
						} else {
							qe = qo.getOwnedExecution();
						}					
					} else {
						String namedNode = qem.namedDest();
						qe = namedNodeQueries.get(new Pair<Integer,String>(queryId,namedNode));
					}
					if (qe == null && (! qem.centralDest()) && (! finishedQueries.contains(queryId)) &&
							(qem.namedDest() == null || this.localNames.contains(qem.namedDest()))) {
						Collection<QueryExecutionMessage> msgs = pendingExecutionMessages.get(queryId);
						if (msgs == null) {
							msgs = new ArrayList<QueryExecutionMessage>();
							pendingExecutionMessages.put(queryId, msgs);
						}
						msgs.add(qem);
						qem = null;
					}
				}
				if (qem != null) {
					if (qe == null) {
						sendMessage(new DoesNotHaveQuery(m, queryId));
					} else {
						qe.process(qem);
					}
				}
			} else if (m instanceof DistributeQueryMessage) {
				DistributeQueryMessage dqm = (DistributeQueryMessage) m;
				QueryPlanWithSchemas<M> qpws;

				Map<String,QpSchema> tableSchemas = store.getTables();

				final int dieDelayMs = this.dieDelayMs;

				try {
					Document doc = QueryPlan.db.get().parse(dqm.getQPWSInputSource());
					qpws = QueryPlanWithSchemas.deserialize(doc.getDocumentElement(), tableSchemas, metadataClass);
				} catch (Exception e) {
					logger.error("Error parsing query plan", e);
					sendMessage(new ReplyException(m,"Error parsing query plan", e, false));
					return;
				}

				try {
					List<QpSchema> schemas = new ArrayList<QpSchema>(tableSchemas.values());
					schemas.addAll(qpws.querySchemas.values());
					QueryExecution<M> qe = new QueryExecution<M>(dqm.queryRouter, dqm.epoch, dqm.queryId, schemas,
							this, dqm.getOrigin(), true, qpws.qp, dqm.config);

					qpws.qp.createDistributedExecution(qe);

					QueryExecution<M> prevQuery = null;
					synchronized (ownedQueries) {
						distributedQueries.put(dqm.queryId, qe);
						if (dqm.restartingPreviousQuery >= 0) {
							prevQuery = distributedQueries.remove(dqm.restartingPreviousQuery);
							finishedQueries.add(dqm.restartingPreviousQuery);
						}
					}
					if (logger.isInfoEnabled()) {
						logger.info("Participant " + localAddr + " created distributed execution for query " + dqm.queryId);
					}
					if (prevQuery != null) {
						qe.setInitialSentDataCount(prevQuery);
						prevQuery.close();
						if (logger.isInfoEnabled()) {
							logger.info("Participant " + localAddr + " closed distributed execution for query " + dqm.restartingPreviousQuery);
						}
					} else if (startCountsForQueries.containsKey(dqm.queryId)) {
						qe.setInitialSentDataCount(startCountsForQueries.get(dqm.queryId));
					}

					Map<String,QueryExecution<M>> namedExecs = new HashMap<String,QueryExecution<M>>();
					for (String name : localNames) {
						QueryExecution<M> namedQE = new QueryExecution<M>(dqm.queryRouter, dqm.epoch, dqm.queryId, schemas,
								this, dqm.getOrigin(), name, qpws.qp, dqm.config);
						qpws.qp.createNamedExecution(name, namedQE);
						synchronized (ownedQueries) {
							namedNodeQueries.put(new Pair<Integer,String>(dqm.queryId,name), namedQE);
							if (dqm.restartingPreviousQuery >= 0) {
								prevQuery = namedNodeQueries.remove(new Pair<Integer,String>(dqm.restartingPreviousQuery,name));
							}
						}
						namedExecs.put(name, namedQE);
						namedQE.start();
						if (logger.isInfoEnabled()) {
							logger.info("Participant " + localAddr + " created named node " + name + " query " + dqm.queryId);
						}
						if (prevQuery != null) {
							qe.setInitialSentDataCount(prevQuery);
							prevQuery.close();
							if (logger.isInfoEnabled()) {
								logger.info("Participant " + localAddr + " closed named node " + name + "  execution for query " + dqm.restartingPreviousQuery);
							}
						}
					}
					qe.start();
					sendReplySuccess(dqm);

					if (logger.isInfoEnabled()) {
						logger.info("Participant " + localAddr + " created distributed query " + dqm.queryId);
					}

					if (dieDelayMs >= 0) {
						timer.schedule(new TimerTask() {

							@Override
							public void run() {
								logger.warn("Stopping participant " + localAddr);
								try {
									stop();
								} catch (Exception e) {
									logger.error("Error stopping participant " + localAddr, e);
									return;
								}
								logger.warn("Stopped participant " + localAddr);
							}

						}, dieDelayMs);

						logger.warn("Participant " + localAddr + " will die in " + dieDelayMs + " ms");
					}
					Collection<QueryExecutionMessage> bufferedMessages;
					synchronized (ownedQueries) {
						bufferedMessages = pendingExecutionMessages.remove(dqm.queryId);
					}
					if (bufferedMessages != null) {
						for (QueryExecutionMessage qem : bufferedMessages) {
							if (qem.distributedDest()) {
								qe.process(qem);
							} else if (qem.centralDest()) {
								throw new IllegalStateException("Shouldn't have central dest messages in buffered messages list");
							} else {
								String name = qem.namedDest();
								QueryExecution<M> namedQE = namedExecs.get(name);
								if (namedQE == null) {
									sendMessage(new DoesNotHaveQuery((QpMessage) qem, dqm.queryId));
								} else {
									namedQE.process(qem);
								}
							}
						}
					}
				} catch (Operator.OperatorCreationException e) {
					logger.error("Error instantiating query plan", e);
					sendMessage(new ReplyException(dqm, "Error instantiating query plan", e, false));
				}
			} else if (m instanceof BeginNewQueryPhase) {
				BeginNewQueryPhase bnqp = (BeginNewQueryPhase) m;
				boolean failed = false;
				synchronized (distributedQueries) {
					QueryExecution<?> qe = distributedQueries.get(bnqp.queryId);
					if (qe == null) {
						failed = true;
					} else {
						qe.beginNewQueryPhase(bnqp);
					}
				}
				for (String name : localNames) {
					QueryExecution<?> qe = namedNodeQueries.get(new Pair<Integer,String>(bnqp.queryId,name));
					if (qe != null) {
						qe.beginNewQueryPhase(bnqp);
					}
				}
				if (failed) {
					sendMessage(new DoesNotHaveQuery(m, bnqp.queryId));
				} else {
					sendReplySuccess(m);
				}
			} else if (m instanceof DistributeQueryContinueMessage) {
				DistributeQueryContinueMessage dqcm = (DistributeQueryContinueMessage) m;
				QueryExecution<M> qe = null; 
				synchronized (distributedQueries) {
					qe = distributedQueries.get(dqcm.queryId);
					if (qe == null) {
						logger.error("Query Engine is null");
					}
				}
				qe.continueEpoch(dqcm.epoch);
				sendReplySuccess(dqcm);
			} else if (m instanceof TearDownQueryMessage) {
				TearDownQueryMessage tdqm = (TearDownQueryMessage) m;
				QueryExecution<M> qe;
				long sentCount = 0;
				synchronized (ownedQueries) {
					qe = distributedQueries.remove(tdqm.queryId);
					finishedQueries.add(tdqm.queryId);
				}
				if (qe != null) {
					sentCount = qe.close();
				}
				for (String name : localNames) {
					synchronized (ownedQueries) {
						qe = namedNodeQueries.remove(new Pair<Integer,String>(tdqm.queryId,name));
					}
					if (qe != null) {
						qe.close();
					}
				}
				sendMessage(new QueryTornDown(tdqm, sentCount));
			} else if (m instanceof LocalRelationIs) {
				QpMessage reply;
				try {
					LocalRelationIs lri = (LocalRelationIs) m;
					String relName = store.getSchema(lri.relationId).getName();
					store.clearTable(relName);
					store.addTuples(lri.getTuples(store, mdf).iterator(), lri.epoch);
					reply = new ReplySuccess(m);
				} catch (Exception e) {
					logger.error("Error storing replicated relation", e);
					reply = new ReplyException(m,"Error storing replicated relation", e,false);
				}
				sendMessage(reply);
			} else if (m instanceof CheckRelation) {
				CheckRelation cr = (CheckRelation) m;
				Set<QpTupleKey> missing = findMissingLocalTuples(cr.relId, cr.epoch, cr.router.getOwnedRanges(localAddr));
				QpMessage mta = new MissingTuplesAre(cr,missing);
				sendMessage(mta);
			} else if (m instanceof GetNodeInfo) {
				GetNodeInfo gni = (GetNodeInfo) m;
				// Make sure we don't process the same request more than once
				final MsgStatus status = msgStatus.getStatus(gni.origFrom, gni.origMsgId); 
				if (status == MsgStatus.UNKNOWN || status == MsgStatus.RECEIVED) {
					// Send local info back to the requester
					QpMessage reply = new NodeInfoIs(gni, nodeId, localAddr);
					if (logger.isTraceEnabled()) {
						logger.trace(localAddr + " sent node info " + reply.messageId + " to " + gni.origFrom + "(" + gni.origMsgId + ")");
					}
					sendMessage(reply);

					// Send on to other nodes
					Collection<InetSocketAddress> nodes;
					synchronized (knownNodes) {
						nodes = new ArrayList<InetSocketAddress>(knownNodes.size());
						for (NodeInfoWithLiveness niwl : knownNodes.values()) {
							if (niwl.alive) {
								nodes.add(niwl.address);
							}
						}

					}
					for (InetSocketAddress node : nodes) {
						this.sendMessage(new GetNodeInfo(node, gni.origFrom, gni.origMsgId));
					}
				}
			} else if (m instanceof GarbageCollect) {
				System.gc();
				sendReplySuccess(m);
			} else if (m instanceof SendKnownNodes) {
				SendKnownNodes skn = (SendKnownNodes) m;
				List<NodeInfo> toUse;
				synchronized (knownNodes) {
					for (NodeInfoWithLiveness niwl : skn.info) {
						NodeInfoWithLiveness already = knownNodes.get(niwl.address);
						if (already == null) {
							knownNodes.put(niwl.address, niwl);
						} else if (! already.id.equals(niwl.id)) {
							logger.warn("Address for " + niwl.address + " has changed from " + already.id + " to " + niwl.id);
							knownNodes.put(niwl.address, niwl);
						} else if (already.alive == false && niwl.alive == true) {
							already.alive = true;
						}
					}
					toUse =  new ArrayList<NodeInfo>(knownNodes.size());
					for (NodeInfoWithLiveness niwl :  knownNodes.values()) {
						if (niwl.alive) {
							toUse.add(niwl.getNodeInfo());
						}
					}
				}
				router = Router.createRouter(toUse, replicationFactor, routerType);
			} else if (m instanceof ConnectMessage) {
				ConnectMessage cm = (ConnectMessage) m;
				final InetSocketAddress from = cm.getOrigin();
				final Collection<NodeInfoWithLiveness> infoForReply;
				synchronized (knownNodes) {
					NodeInfoWithLiveness already = knownNodes.get(from);
					if (already == null) {
						knownNodes.put(from, new NodeInfoWithLiveness(cm.senderId, from));
					} else if (! already.id.equals(cm.senderId)) {
						logger.warn("Address for " + from + " has changed from " + already.id + " to " + cm.senderId);
						knownNodes.put(from, new NodeInfoWithLiveness(cm.senderId, from));
					} else if (already.alive == false) {
						already.alive = true;
					}
					infoForReply = new ArrayList<NodeInfoWithLiveness>(knownNodes.values());
				}
				this.sendMessage(new SendKnownNodes(from, infoForReply));
			} else if (m instanceof DummyMessage) {
				if (((DummyMessage) m).wantReply) {
					sendMessage(new ReplySuccess(m));
				}
			} else {
				logger.error("QpApplication doesn't know what to do with message " + m);
			}
		} catch (Exception e) {
			logger.error("Error in message processing thread", e);
			if (m.getOrigin() != null) {
				try {
					sendMessage(new ReplyException(m,"Error processing message",e,true));
				} catch (IOException ioe) {
					logger.error("Error sending ReplyException", ioe);
				} catch (InterruptedException ie) {
					return;
				}
			}
		}
	}

	public void startedThrottling(InetSocketAddress node) {
		if (! router.getParticipants().contains(node)) {
			return;
		}

		List<ReplyData> toStop;
		synchronized (messageDests) {
			Set<Long> messageIds = messageDests.get(node);
			if (messageIds == null) {
				return;
			}
			toStop = new ArrayList<ReplyData>(messageIds.size());
			for (Long id : messageIds) {
				ReplyData rd = replyData.get(id);
				if (rd != null) {
					toStop.add(rd);
				}
			}
		}

		for (ReplyData rd : toStop) {
			rd.cancelPendingAction();
		}

		logger.info(localAddr + " stopped sending to " + node);
	}

	public void stoppedThrottling(InetSocketAddress node) {
		List<ReplyData> toStart;
		synchronized (messageDests) {
			Set<Long> messageIds = messageDests.get(node);
			if (messageIds == null) {
				return;
			}
			toStart = new ArrayList<ReplyData>(messageIds.size());
			for (Long id : messageIds) {
				ReplyData rd = replyData.get(id);
				if (rd != null) {
					toStart.add(rd);
				}
			}
		}
		try {
			for (ReplyData rd : toStart) {
				rd.startPendingAction(this);
			}
		} catch (InterruptedException ie) {
			return;
		} catch (Exception e) {
			logger.error("Error stopping throttling", e);
		}
		logger.info(localAddr + " started sending to " + node);
	}

	private class MessageProcessingThread extends Thread {
		QpMessage m;
		private boolean interruptable = true;
		private boolean interrupted = false;
		private MessageProcessingThread(int count) {
			super(tg, "Message Processing Thread #" + count + " (" + localAddr + ")");
		}

		public synchronized boolean isInterrupted() {
			return interrupted;
		}

		public synchronized void interrupt() {
			interrupted = true;
			if (interruptable) {
				super.interrupt();
			}
		}

		public void run() {
			try {
				while (! isInterrupted()) {
					final QpMessage currMsg = socketManager.readMessage();
					synchronized (this) {
						m = currMsg;
						interruptable = false;
					}

					try {
						processMessage(currMsg);
						if (logger.isInfoEnabled()) {
							String msgClass = removePackageName(m.getClass().getName());
							synchronized (messageCounts) {
								Record rec = messageCounts.get(msgClass);
								if (rec == null) {
									messageCounts.put(msgClass, new Record(msgClass));
								} else {
									rec.increment();
								}
							}
						}
					} catch (Exception e) {
						logger.error("Error processing " + currMsg, e);
					} finally {
						synchronized (this) {
							m = null;
							interruptable = true;
						}
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}

		synchronized boolean isBusy() {
			return (m != null);
		}
	}


	/**
	 * Send a message, after updating the reply status cache.
	 * After this method returns, the message destination, if known,
	 * will be set.
	 * 
	 * @param m			The message to send
	 * @throws IOException
	 * @throws InterruptedException
	 */
	void sendMessage(QpMessage m) throws IOException, InterruptedException {
		if (m.isReply()) {
			final InetSocketAddress isa = m.getDest();
			if (m instanceof ReplyException || m instanceof ReplyFailure) {
				for (long msgId : m.getOrigIds()) {
					msgStatus.recordMessageFailure(isa, msgId, m.canRetry());
				}
			} else if (m instanceof ReplySuccess) {
				for (long msgId : m.getOrigIds()) {
					msgStatus.recordMessageSuccess(isa, msgId);
				}
			} else {
				for (long msgId : m.getOrigIds()) {
					msgStatus.recordMessageFinished(isa, msgId, m.canRetry());
				}
			}
		}
		send(m);
	}

	/**
	 * Send a message. After this method returns, the message destination, if known,
	 * will be set.
	 * 
	 * @param m			The message to send
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void send(QpMessage m) throws IOException, InterruptedException {
		m.send(localAddr, router, socketManager);
	}

	void sendReplySuccess(QpMessage received) throws IOException, InterruptedException {
		msgStatus.recordMessageSuccess(received.getOrigin(), received.messageId);
		QpMessage m = new ReplySuccess(received);
		send(m);
	}

	TimerTask scheduleDeliverMessage(final QpMessage m, int delayMs) {
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				socketManager.deliverMessage(m);
			}

		};
		timer.schedule(task, delayMs);
		return task;
	}
	
	TimerTask scheduleDeliverRepeatedMessage(final QpMessage m, int delayMs) {
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				socketManager.deliverMessage(m);
			}

		};
		timer.schedule(task, delayMs, delayMs);
		return task;
	}
	

	TimerTask scheduleResendMessage(final ReplyData rd, int delayMs) {
		TimerTask task = new TimerTask() {

			@Override
			public void run() {
				try {
					rd.resend(QpApplication.this);
				} catch (InterruptedException e) {
					return;
				} catch (Exception e) {
					try {
						logger.error("Error sending message " + retrieveSentMessage(rd.msgId), e);
					} catch (SerializationException e1) {
						logger.error("Error deserializing sent message #" + rd.msgId, e1);
					}
				}
			}

			public boolean cancel() {
				if (super.cancel()) {
					try {
						logger.info("Cancelling resend of " + retrieveSentMessage(rd.msgId));
					} catch (SerializationException e) {
						logger.error("Error deserializing sent message #" + rd.msgId, e);
					}
					return true;
				} else {
					return false;
				}
			}
		};
		timer.schedule(task, delayMs);
		return task;
	}

	void sendMessageAwaitReply(QpMessage m, ReplyContinuation rc, int retryDelay, int numRetries, int replyTimeout, Class<?>... successReplies) throws IOException, InterruptedException {
		ReplyData rd = new ReplyData(rc, m.messageId, m.retryable(), retryDelay, numRetries, replyTimeout, successReplies);
		this.storeSentMessage(m);
		synchronized (replyData) {
			replyData.put(m.messageId, rd);
		}
		sendMessageAwaitReply(m, rd);
	}

	void sendMessageAwaitReply(ReplyData rd) throws IOException, InterruptedException, SerializationException {
		sendMessageAwaitReply(this.retrieveSentMessage(rd.msgId), rd);
	}

	void sendMessageAwaitReply(QpMessage m, ReplyData rd) throws IOException, InterruptedException {
		sendMessage(m);
		if (m instanceof QpMessage) {
			InetSocketAddress dest = ((QpMessage) m).getDest();
			if (dest != null) {
				synchronized (rd) {
					rd.setSentTo(dest);
					synchronized (messageDests) {
						Set<Long> msgs = messageDests.get(dest);
						if (msgs == null) {
							msgs = new HashSet<Long>();
							messageDests.put(dest, msgs);
						}
						msgs.add(m.messageId);
					}
				}
			}
		} else {
			rd.setSent();
			try {
				rd.startPendingAction(this);
			} catch (SerializationException e) {
				logger.error("Error sending message", e);
			}
		}
	}

	void sendMessageAwaitReply(QpMessage m, ReplyContinuation rc,
			Class<?>... successReplies) throws IOException, InterruptedException {
		sendMessageAwaitReply(m, rc, DEFAULT_RETRY_DELAY, DEFAULT_NUM_RETRIES, 0, successReplies);
	}

	public MetadataFactory<M> getMetadataFactory() {
		return mdf;
	}

	public void addTable(QpSchema schema, int epoch) throws IllegalArgumentException {
		QpSchema.Location l = schema.getLocation();
		if (l != QpSchema.Location.REPLICATED && l != QpSchema.Location.STRIPED) {
			throw new IllegalArgumentException("Schema " + schema.getName() + " is neither replicated nor striped");
		}
		store.addTable(schema);

		if (l == QpSchema.Location.REPLICATED) {
			return;
		}

		Set<Integer> keyCols = schema.getKeyColsSet();

		for (int col : schema.getHashCols()) {
			if (! keyCols.contains(col)) {
				throw new IllegalArgumentException("Table must have only key columns as hash columns");
			}
		}

		final int table = schema.relId;
		synchronized (firstEpochsForTables) {
			Integer e = firstEpochsForTables.get(table);
			if (e != null) {
				throw new IllegalArgumentException("Already have first epoch (" + e + ") for table " + table + " when trying to add new first epoch (" + epoch + ")");
			}
			firstEpochsForTables.put(table, epoch);
		}
	}

	public void addViewSchema(QpSchema schema) throws IllegalArgumentException {
		store.addTable(schema);
	}

	int getFirstEpochForTable(int table) {
		synchronized (firstEpochsForTables) {
			Integer epoch = firstEpochsForTables.get(table);
			if (epoch == null) {
				throw new IllegalArgumentException("Don't have a first epoch for table " + table);
			}
			return epoch;
		}
	}

	void clear() throws TupleStoreException, DHTException, InterruptedException, IOException {
		if (msgStatus != null) {
			msgStatus.clear();	
		}
		store.clear();
		dht.clear();

		firstEpochsForTables.clear();
		replyData.clear();
	}

	public Id getNodeId() {
		return nodeId;
	}

	public void beginQuery(int queryId, int epoch, QueryPlan<M> qp, Collection<? extends QpSchema> schemas)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, new QueryPlanWithSchemas<M>(qp,schemas,metadataClass), new Configuration(), -1, null);
	}

	public void beginQuery(int queryId, int epoch, QueryPlan<M> qp, Collection<? extends QpSchema> schemas, Configuration config)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, new QueryPlanWithSchemas<M>(qp,schemas,metadataClass), config, -1, null);
	}

	void beginQuery(int queryId, int epoch, QueryPlan<M> qp, Collection<? extends QpSchema> schemas, Configuration config, int prevQueryId, Router routerToUse)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, new QueryPlanWithSchemas<M>(qp,schemas,metadataClass), config, prevQueryId, routerToUse);
	}

	public void beginQueryFromNonMetadataPlan(final int queryId, int epoch, QueryPlanWithSchemas<Null> qpws, Configuration config)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, qpws.convertNullPlan(metadataClass), config, -1, null);
	}
	
	public void beginQueryFromNonMetadataPlan(final int queryId, int epoch, QueryPlanWithSchemas<Null> qpws)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, qpws.convertNullPlan(metadataClass), new Configuration(), -1, null);
	}
	
	public void beginQuery(final int queryId, int epoch, QueryPlanWithSchemas<M> qpws, Configuration config)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, qpws, config, -1, null);
	}
	
	public void beginQuery(final int queryId, int epoch, QueryPlanWithSchemas<M> qpws, Configuration config, Set<InetSocketAddress> blacklist)
	throws InterruptedException, QueryInstantiationException {
		Router r = null;
		if (blacklist != null && (! blacklist.isEmpty())) {
			r = this.router.getRouterWithout(blacklist);
			System.out.println("Creating router for query " + queryId + ": " + r);
		}
		beginQuery(queryId, epoch, qpws, config, -1, r);
	}
	
	public void beginQuery(final int queryId, int epoch, QueryPlanWithSchemas<M> qpws)
	throws InterruptedException, QueryInstantiationException {
		beginQuery(queryId, epoch, qpws, new Configuration(), -1, null);
	}
	
	void beginQuery(final int queryId, int epoch, QueryPlanWithSchemas<M> qpws, Configuration config, int prevQueryId, Router routerToUse)
	throws InterruptedException, QueryInstantiationException {
		try {
			final Router queryRouter = (routerToUse != null) ? routerToUse : router;

			if (prevQueryId >= 0) {
				logger.info("Starting query " + queryId + " as replacement for " + prevQueryId);			
				QueryOwner<M> oldOwner;
				synchronized (ownedQueries) {
					oldOwner = ownedQueries.remove(prevQueryId);
				}
				oldOwner.close();
			} else {
				logger.info("Starting query " + queryId);
			}

			Map<String,QpSchema> baseTables = store.getTables();

			final QueryOwner<M> qo = new QueryOwner<M>(queryRouter, queryId, this, epoch, qpws.qp, baseTables.values(), qpws.querySchemas.values(), getParticipants(), config);

			synchronized (ownedQueries) {
				ownedQueries.put(queryId, qo);
			}

			Document doc = QueryPlan.db.get().newDocument();
			Element el = doc.createElement("queryPlanAndSchema");
			doc.appendChild(el);
			qpws.serialize(doc, el, baseTables);
			StringWriter sw = new StringWriter();
			DomUtils.write(doc, sw);

			DistributeQueryMessage dqm = null;

			for (final InetSocketAddress dest : queryRouter.getParticipants()) {
				if (dqm == null) {
					dqm = new DistributeQueryMessage(dest, sw.toString(), epoch, queryId, queryRouter, config, prevQueryId);
				} else {
					dqm = dqm.retarget(dest);
				}
				sendMessageAwaitReply(dqm, new ReplyContinuation() {
					private boolean finished = false;
					public synchronized boolean isFinished() {
						return finished;
					}

					public synchronized void processReply(QpMessage m) {
						finished = true;
						if (m instanceof ReplyException) {
							qo.reportException(((ReplyException) m).why);
						} else if (m instanceof ReplyTimeout) {
							logger.info("Timeout in reply to DistributeQueryMessage(" + queryId + ") to node " + dest);
							qo.nodesHaveFailed(Collections.singleton(dest));
						} else if (!(m instanceof ReplySuccess)) {
							qo.reportException(new RuntimeException("Received unexpected reply to DistributeQueryMessage from " + dest + ": " + m));
						}
					}
				}, 0, 0, QUERY_DISSEMINATION_TIME_MS, QpMessage.class);
			}

			logger.info("Starting query owner for query " + queryId);
			qo.start();
		} catch (InterruptedException ie) {
			throw ie;
		} catch (Exception e) {
			throw new QueryInstantiationException(e);
		}
	}

	public void continueQuery(final int queryId, int currentEpoch) throws InterruptedException, 
	QueryInstantiationException, IOException, DHTException{
		logger.info("Continuing query " + queryId);
		final QueryOwner<?> qo;
		synchronized (ownedQueries) {
			qo = ownedQueries.get(queryId);
		}
		if (qo == null) {
			throw new IllegalArgumentException("Don't have owner query with id " + queryId);
		}

		final Router r = router;
		final Set<InetSocketAddress> remaining = new HashSet<InetSocketAddress>(r.getParticipants());
		final Map<InetSocketAddress,QpMessage> errors = new HashMap<InetSocketAddress,QpMessage>();

		DistributeQueryContinueMessage dqm = null;

		for (final InetSocketAddress dest : r.getParticipants()) {
			dqm = new DistributeQueryContinueMessage(dest, currentEpoch, queryId);
			sendMessageAwaitReply(dqm, new ReplyContinuation() {
				private boolean finished = false;
				public synchronized boolean isFinished() {
					return finished;
				}

				public synchronized void processReply(QpMessage m) {
					finished = true;
					synchronized (remaining) {
						remaining.remove(dest);
						if (! (m instanceof ReplySuccess)) {
							errors.put(dest, m);
						}
						if (remaining.isEmpty()) {
							remaining.notify();
						}
					}
				}

			}, 0, 0, QUERY_DISSEMINATION_TIME_MS, QpMessage.class);
		}

		synchronized (remaining) {
			while (! remaining.isEmpty()) {
				remaining.wait();
			}
		}

		if (! errors.isEmpty()) {
			throw new QueryInstantiationException(errors.toString());
		}

		logger.info("Continuing query owner for query " + queryId);
		qo.continueQueryOwner(currentEpoch);
	}

	public long endQuery(int queryId) throws InterruptedException, IOException {
		while (restartedQueries.containsKey(queryId)) {
			queryId = restartedQueries.get(queryId);
		}
		QueryOwner<?> qo;
		synchronized (ownedQueries) {
			qo = ownedQueries.remove(queryId);
			finishedQueries.add(queryId);
		}
		long localCount = qo.closeOwner();

		Router r = qo.getOwnedExecution().getRouter(qo.getCurrentPhase());

		final Map<InetSocketAddress,Long> sentData = new HashMap<InetSocketAddress,Long>();
		final Set<InetSocketAddress> participants = new HashSet<InetSocketAddress>(r.getParticipants());
		participants.removeAll(qo.getReportedFailedNodes());

		for (final InetSocketAddress dest : participants) {
			sendMessageAwaitReply(new TearDownQueryMessage(dest,queryId), new ReplyContinuation() {
				private boolean finished = false;
				@Override
				public synchronized boolean isFinished() {
					return finished;
				}

				@Override
				public synchronized void processReply(QpMessage m) {
					finished = true;
					synchronized (sentData) {
						if (m instanceof QueryTornDown) {
							sentData.put(dest, ((QueryTornDown) m).totalBytesSent);
						} else {
							sentData.put(dest, 0L);
						}
						if (sentData.size() == participants.size()) {
							sentData.notify();
						}
					}
				}
			}, QueryTornDown.class);
		}

		synchronized (sentData) {
			while (sentData.size() < participants.size()) {
				sentData.wait();
			}
		}

		long totalSent = localCount;
		for (Map.Entry<InetSocketAddress, Long> me : sentData.entrySet()) {
			if (! me.getKey().equals(this.localAddr)) {
				totalSent += me.getValue();
			}
		}
		return totalSent;
	}

	public List<QpTuple<Null>> getQueryResultWithoutMetadata(int queryId) throws InterruptedException, QueryFailure, IOException {
		for ( ; ; ) {
			QueryOwner<?> qo;
			synchronized (ownedQueries) {
				qo = ownedQueries.get(queryId);
			}
			if (qo == null) {
				throw new IllegalArgumentException("Don't have owner query with id " + queryId);
			}
			try {
				qo.waitUntilDone();
				return qo.getSpoolOperator().getResultsWithoutMetadata();
			} catch (Exception e) {
				Integer redirect = restartedQueries.get(queryId);
				if (redirect != null) {
					queryId = redirect;
					continue;
				}
				if (e instanceof QueryFailure) {
					throw (QueryFailure) e;
				} else if (e instanceof InterruptedException) {
					throw (InterruptedException) e;
				} else if (e instanceof IOException) {
					throw (IOException) e;
				}
				QueryFailure qf = new QueryFailure("Caught unexpected exception", e);
				throw qf;
			}
		}
	}

	public int getQueryResultCardinality(int queryId) throws InterruptedException, QueryFailure, IOException {
		for ( ; ; ) {
			QueryOwner<?> qo;
			synchronized (ownedQueries) {
				qo = ownedQueries.get(queryId);
			}
			if (qo == null) {
				throw new IllegalArgumentException("Don't have owner query with id " + queryId);
			}
			try {
				qo.waitUntilDone();
				return qo.getSpoolOperator().getResultCardinality();
			} catch (Exception e) {
				Integer redirect = restartedQueries.get(queryId);
				if (redirect != null) {
					queryId = redirect;
					continue;
				}
				if (e instanceof QueryFailure) {
					throw (QueryFailure) e;
				} else if (e instanceof InterruptedException) {
					throw (InterruptedException) e;
				} else if (e instanceof IOException) {
					throw (IOException) e;
				}
				QueryFailure qf = new QueryFailure("Caught unexpected exception", e);
				throw qf;
			}
		}
	}

	final Map<Integer,Integer> restartedQueries = Collections.synchronizedMap(new HashMap<Integer,Integer>());

	public List<QpTuple<M>> getQueryResultWithMetadata(int queryId) throws InterruptedException, QueryFailure, IOException {
		QueryOwner<M> qo;
		for ( ; ; ) {
			synchronized (ownedQueries) {
				qo = ownedQueries.get(queryId);
			}
			if (qo == null) {
				throw new IllegalArgumentException("Don't have owner query with id " + queryId);
			}
			try {
				qo.waitUntilDone();
				return qo.getSpoolOperator().getResultsWithMetadata();
			} catch (Exception e) {
				Integer redirect = restartedQueries.get(queryId);
				if (redirect != null) {
					queryId = redirect;
					continue;
				}
				if (e instanceof QueryFailure) {
					throw (QueryFailure) e;
				} else if (e instanceof InterruptedException) {
					throw (InterruptedException) e;
				} else if (e instanceof IOException) {
					throw (IOException) e;
				}
				QueryFailure qf = new QueryFailure("Caught unexpected exception", e);
				throw qf;
			}
		}
	}

	public TupleStore<M> getStore() {
		return this.store;
	}

	public void addLocalName(String name) {
		this.localNames.add(name);
	}

	public void removeLocalName(String name) {
		this.localNames.remove(name);
	}

	public void clearLocalNames() {
		this.localNames.clear();
	}

	InetSocketAddress getNamedNodehandle(String name) {
		InetSocketAddress isa = allNames.get(name);
		if (isa == null) {
			throw new IllegalArgumentException("Don't know location of named node " + name);
		}
		return isa;
	}

	public DHTService<M> getDHT() {
		return this.dht;
	}

	public void printStoreStats(PrintStream ps) throws Exception {
		store.printStats(ps);
	}

	public void publishReplicatedRelation(int relationId, QpTupleBag<M> tuples) throws IOException, InterruptedException {
		final Set<InetSocketAddress> remaining = new HashSet<InetSocketAddress>(router.getParticipants());
		final Map<InetSocketAddress,QpMessage> errors = new HashMap<InetSocketAddress,QpMessage>();
		LocalRelationIs msg = null;
		for (final InetSocketAddress dest : router.getParticipants()) {
			if (msg == null) {
				msg = new LocalRelationIs(dest, store.getSchema(relationId), tuples, 0, mdf);
			} else {
				msg = msg.retarget(dest);
			}
			sendMessageAwaitReply(msg, new ReplyContinuation() {
				private boolean finished = false;
				public synchronized boolean isFinished() {
					synchronized (remaining) {
						return finished;
					}
				}

				public void processReply(QpMessage m) {
					synchronized (this) {
						finished = true;
					}
					synchronized (remaining) {
						if (! (m instanceof ReplySuccess)) {
							errors.put(dest,m);
						}
						remaining.remove(dest);
						if (remaining.isEmpty()) {
							remaining.notify();
						}
					}
				}
			}, ReplySuccess.class);
		}

		synchronized (remaining) {
			while (! remaining.isEmpty()) {
				remaining.wait();
			}
		}

		if (! errors.isEmpty()) {
			logger.error("Received errors in reply to LocalRelationIs: " + errors);
		}
	}

	public Map<InetSocketAddress,Set<QpTupleKey>> findMissingTuples(String relation, int epoch) throws InterruptedException, IOException {
		final Map<InetSocketAddress,Set<QpTupleKey>> retval = new HashMap<InetSocketAddress,Set<QpTupleKey>>();
		final QpSchema schema = store.getSchema(relation);

		final Router r = router;
		final Set<InetSocketAddress> remaining = new HashSet<InetSocketAddress>(r.getParticipants());
		final Map<InetSocketAddress,QpMessage> errors = new HashMap<InetSocketAddress,QpMessage>();

		for (final InetSocketAddress node : r.getParticipants()) {
			QpMessage m = new CheckRelation(node,router,schema.relId,epoch);

			ReplyContinuation rc = new ReplyContinuation() {
				private boolean finished = false;
				public synchronized boolean isFinished() {
					return finished;
				}

				public void processReply(QpMessage m) {
					synchronized (this) {
						finished = true;
					}
					synchronized (remaining) {
						if (m instanceof MissingTuplesAre) {
							remaining.remove(node);
							if (remaining.isEmpty()) {
								remaining.notify();
							}
							InetSocketAddress addr = m.getOrigin();
							Set<QpTupleKey> missing = ((MissingTuplesAre) m).getData(schema);
							if (! missing.isEmpty()) {
								retval.put(addr, missing);
							}
						} else {
							errors.put(node, m);
						}
					}
				}

			};
			sendMessageAwaitReply(m, rc, 0, 0, 240000, QpMessage.class);
		}


		synchronized (remaining) {
			while (! remaining.isEmpty()) {
				remaining.wait();
			}
		}

		if (! errors.isEmpty()) {
			logger.error("Received errors in reply to LocalRelationIs: " + errors);
		}


		return retval;
	}

	private Set<QpTupleKey> findMissingLocalTuples(int relId, int epoch, final IdRangeSet ownedRanges) throws InterruptedException {
		final HashSet<QpTupleKey> missing = new HashSet<QpTupleKey>();
		final Holder<Boolean> done = new Holder<Boolean>(false);

		dht.getTuplesInRelation(relId, epoch, new KeyTupleSink() {
			int count = 0;
			Set<Integer> remaining = new HashSet<Integer>();
			boolean deliveredAll = false;
			int numPagesRemaining = -1;
			public void deliverTuples(QpTupleKey[] contents) {
				final ArrayList<QpTupleKey> toFind = new ArrayList<QpTupleKey>(contents.length);
				for (QpTupleKey t : contents) {
					if (t == null) {
						break;
					}
					if (ownedRanges.contains(t.getQPid())) {
						toFind.add(t);
					}
				}
				final int thisCount;
				synchronized (missing) {
					if (numPagesRemaining < 0) {
						processException(new RuntimeException("numPagesRemaining < 0, is totalNumTuplesIs not being called?"));
						return;
					}
					--numPagesRemaining;
					if (numPagesRemaining < 0) {
						processException(new RuntimeException("numPagesRemaining < 0, pages are being delivered multiple times"));
						return;
					}					
					if (numPagesRemaining == 0) {
						deliveredAll = true;
					}
					if (toFind.isEmpty()) {
						if (deliveredAll) {
							done.value = true;
							missing.notify();
						}
						return;
					} else {
						thisCount = count++;
						remaining.add(thisCount);
					}
				}
				try {
					store.getTuplesByKey(toFind.iterator(), new TupleStore.TupleSink<M>() {
						int numRemaining = toFind.size();
						public synchronized void deliverTuple(QpTuple<M> tuple) {
							--numRemaining;
							if (numRemaining == 0) {
								synchronized (missing) {
									remaining.remove(thisCount);
									if (deliveredAll && remaining.isEmpty()) {
										done.value = true;
										missing.notify();
									}
								}
							}
						}

						public synchronized void tupleNotFound(QpTupleKey key) {
							--numRemaining;
							synchronized (missing) {
								missing.add(key);
								if (numRemaining == 0) {
									remaining.remove(thisCount);
									if (deliveredAll && remaining.isEmpty()) {
										done.value = true;
										missing.notify();
									}
								}
							}
						}

					});
				} catch (TupleStoreException e) {
					processException(e);
				}
			}

			public void processException(Exception e) {
				e.printStackTrace();
			}

			public void totalNumTuplesIs(int numTuples, int numPages) {
				synchronized (missing) {
					numPagesRemaining = numPages;
				}
			}
		});

		synchronized (missing) {
			while (! done.value) {
				missing.wait();
			}
		}

		if (logger.isInfoEnabled()) {
			logger.info("Finished checking relation " + store.getSchema(relId).getName() + ", found " + missing.size() + " missing tuples");
		}
		return missing;
	}

	public static String removePackageName(String fullyQualified) {
		return fullyQualified.substring(fullyQualified.lastIndexOf('.') + 1);
	}

	public void startForcedThrottling() {
		socketManager.startForcedThrottling();
	}

	public void stopForcedThrottling() {
		socketManager.stopForcedThrottling();
	}

	private interface NodeInfoSink {
		void deliver(List<NodeInfo> result);
		void exception(Exception e);
	}

	private void getNodeInfo(final NodeInfoSink ris) throws IOException, InterruptedException {
		final GetNodeInfo gni = new GetNodeInfo(localAddr, localAddr);

		logger.info("Node " + localAddr + " is sending GetNodeInfo(" + gni.messageId + ")");

		ReplyContinuation rc = new ReplyContinuation() {
			List<NodeInfo> retval = new ArrayList<NodeInfo>();
			Set<InetSocketAddress> processed = new HashSet<InetSocketAddress>();
			{
				// Include local info
				retval.add(new NodeInfo(nodeId, localAddr));
				processed.add(localAddr);
			}

			boolean finished = false;
			public synchronized boolean isFinished() {
				return finished;
			}

			public synchronized void processReply(QpMessage m) {
				if (m instanceof ReplyTimeout) {
					finished = true;
					ris.deliver(retval);
				} else if (m instanceof NodeInfoIs) {
					if (logger.isTraceEnabled()) {
						logger.trace("GetNodeInfo(" + gni.messageId + ") at " + localAddr + " received NodeInfoIs " + m.messageId + " from " + m.getOrigin());
					}
					NodeInfoIs nii = (NodeInfoIs) m;
					if (processed.add(nii.getOrigin())) {
						retval.add(nii.ni);
					}
				} else {
					logger.warn("Received unexpected reply to GetNodeInfo: " + m);
				}
			}

		};

		this.sendMessageAwaitReply(gni, rc, 0, 0, FLOODING_MAX_WAIT_INTERVAL_MS, ReplyTimeout.class, NodeInfoIs.class);
	}

	public synchronized boolean throttlingOccurring() {
		return socketManager.throttlesNodes() || socketManager.isThrottledByNodes();
	}

	public Router getRouter() {
		return router;
	}

	public void sentMessagesReceived(InetSocketAddress sentTo, LongList msgIds) {
		if (logger.isTraceEnabled()) {
			logger.trace("Messages sent from " + localAddr + " to " + sentTo + " have been received: " + msgIds);
		}
		for (long msgId : msgIds.getList()) {
			ReplyData rd = replyData.get(msgId);
			if (rd != null) {
				synchronized (rd) {
					rd.rc.received();
				}
			}
		}
	}

	public Set<InetSocketAddress> getParticipants() {
		return router.getParticipants();
	}

	void removeReplyContinuation(long msgId) {
		ReplyData rd = replyData.remove(msgId);
		if (rd == null) {
			return;
		}
		rd.cancelPendingAction();
	}

	public void messageSendingFailed(InetSocketAddress dest, LongList msgIds) {
		try {
			for (long msgId : msgIds.getList()) {
				ReplyData rd = replyData.get(msgId);
				if (rd == null) {
					continue;
				}
				QpMessage m = new ReplySendingFailed(msgId, rd.retryable);
				processMessage(m);
			}
		} catch (Exception e) {
			logger.error("Error enqueing notification that sending failed", e);
		}
	}

	public void messagesSent(LongList msgIds) {
		try {
			for (long msgId : msgIds.getList()) {
				ReplyData rd = replyData.get(msgId);
				if (rd != null) {
					synchronized (rd) {
						rd.setSent();
						rd.rc.sent();
						rd.startPendingAction(this);
					}
				}
			}
			if (logger.isTraceEnabled()) {
				logger.trace("Node " + localAddr + " sent messages " + msgIds);
			}
		} catch (InterruptedException ie) {
			return;
		} catch (Exception e) {
			logger.error("Error setting message sent", e);
		}
	}

	public void gc() throws IOException, InterruptedException {
		final Set<InetSocketAddress> remaining = new HashSet<InetSocketAddress>(router.getParticipants());
		final Set<InetSocketAddress> timedOut = new HashSet<InetSocketAddress>();
		for (final InetSocketAddress dest : router.getParticipants()) {
			GarbageCollect gc = new GarbageCollect(dest);
			sendMessageAwaitReply(gc, new ReplyContinuation() {
				private boolean finished = false;
				public synchronized boolean isFinished() {
					synchronized (remaining) {
						return finished;
					}
				}

				public void processReply(QpMessage m) {
					synchronized (this) {
						finished = true;
					}
					synchronized (remaining) {
						if (m instanceof ReplyTimeout) {
							timedOut.add(dest);
						}
						remaining.remove(dest);
						if (remaining.isEmpty()) {
							remaining.notify();
						}
					}
				}
			}, ReplySuccess.class);
		}

		synchronized (remaining) {
			while (! remaining.isEmpty()) {
				remaining.wait();
			}
		}

		if (timedOut.isEmpty()) {
			logger.info("Garbage collected at all nodes");
		} else {
			logger.error("Received no reply to GarbageCollection from " + timedOut);
		}
	}

	Set<InetSocketAddress> getFailedNodesForQuery(int queryId) {
		synchronized (ownedQueries) {
			QueryExecution<?> qe = distributedQueries.get(queryId);
			if (qe == null) {
				return null;
			} else {
				return qe.getFailedNodes();
			}
		}
	}

	public void peerIsNotDead(InetSocketAddress peer) {
		synchronized (knownNodes) {
			NodeInfoWithLiveness niwl = knownNodes.get(peer);
			if (niwl != null) {
				niwl.alive = true;
			}
		}
	}

	public void peerIsDead(InetSocketAddress peer) {
		if (logger.isInfoEnabled()) {
			logger.info("Node " + this.localAddr + " learned that " + peer + " has died");
		}
		synchronized (knownNodes) {
			NodeInfoWithLiveness niwl = knownNodes.get(peer);
			if (niwl != null) {
				niwl.alive = false;
			}
		}
		router = router.getRouterWithout(Collections.singleton(peer));
		List<QueryOwner<M>> qos = new ArrayList<QueryOwner<M>>();
		synchronized (ownedQueries) {
			qos.addAll(ownedQueries.values());
		}
		for (QueryOwner<M> qo : qos) {
			qo.nodesHaveFailed(Collections.singleton(peer));
		}
		List<QueryExecution<M>> qes = new ArrayList<QueryExecution<M>>();
		synchronized (ownedQueries) {
			qes.addAll(distributedQueries.values());
			qes.addAll(namedNodeQueries.values());
		}
		for (QueryExecution<?> qe : qes) {
			qe.getRecordTuples().nodesHaveFailed(Collections.singleton(peer));
		}

		Set<Long> msgIds;
		synchronized (messageDests) {
			msgIds = messageDests.remove(peer);
		}
		if (msgIds == null) {
			return;
		}
		List<Long> canRetry = new ArrayList<Long>(), cannotRetry = new ArrayList<Long>();
		try {
			for (Long id : msgIds) {
				ReplyData rd = replyData.get(id);
				if (rd == null || (! rd.retryable)) {
					continue;
				}
				QpMessage m = this.retrieveSentMessage(rd.msgId);
				if (m.hasDestId()) {
					canRetry.add(id);
				} else {
					cannotRetry.add(id);
				}
			}

			if (! canRetry.isEmpty()) {
				processMessage(new MessageDestHasDied(peer,canRetry,true));
			}
			if (! cannotRetry.isEmpty()) {
				processMessage(new MessageDestHasDied(peer,cannotRetry,false));
			}
		} catch (Exception e) {
			logger.error("Error notifying of node death", e);
		}
	}

	void nodesHaveFailed(Set<InetSocketAddress> failedNodes) {
		router = this.router.getRouterWithout(failedNodes);
	}

	private volatile int dieDelayMs = -1;

	public void dieAfterNextQuery(int msDelay) {
		dieDelayMs = msDelay;
	}

	public int getDieDelayMs() {
		return dieDelayMs;
	}

	public DHTService.RelationInfo getRelationInfo(int relId, int epoch) throws InterruptedException, DHTException, IOException {
		return dht.getRelationInfo(relId, epoch);
	}

	public enum RecoveryMode {
		RESTART, INCREMENTAL, ABORT
	}

	boolean queryFinished(int queryId) {
		synchronized (ownedQueries) {
			return finishedQueries.contains(queryId);
		}
	}

	public Router.Type getRouterType() {
		return this.routerType;
	}

	public void setRouterType(Router.Type routerType) {
		if (routerType == null) {
			throw new NullPointerException("routerType cannot be null");
		}
		this.routerType = routerType;
		this.router = Router.createRouter(router.getNodeInfo(), replicationFactor, routerType);
	}

	public static class NodeInfoWithLiveness {
		final Id id;
		final InetSocketAddress address;
		boolean alive;

		NodeInfoWithLiveness(Id id, InetSocketAddress address) {
			this(id,address,true);
		}

		NodeInfoWithLiveness(Id id, InetSocketAddress address, boolean alive) {
			this.id = id;
			this.address = address;
			this.alive = alive;
		}

		NodeInfo getNodeInfo() {
			return new NodeInfo(id, address);
		}

		public void serialize(OutputBuffer buf) {
			id.serialize(buf);
			buf.writeInetSocketAddress(address);
			buf.writeBoolean(alive);
		}

		public static NodeInfoWithLiveness deserialize(InputBuffer buf) {
			Id id = Id.deserialize(buf);
			InetSocketAddress address = buf.readInetSocketAddress();
			boolean alive = buf.readBoolean();
			return new NodeInfoWithLiveness(id, address, alive);
		}
	}

	public static class Configuration {
		public final boolean discardResults;
		public final boolean multipleScanThreads;
		public final int threadsPerDistributedScan;
		public final int scanThreadPriority;
		public final int shipWindowMs;
		public final int sendDelayMs;
		public final int compressionLevel;
		public final RecoveryMode recoveryMode;
		public final boolean createSpecialRecoveryRouter;
		public final boolean bufferReceivedTuples;
		public final int maxBufferedLength;
		public final int maxBufferedWaitMs;
		
		public Configuration() {
			discardResults = false;
			multipleScanThreads = true;
			threadsPerDistributedScan = 5;
			scanThreadPriority = Thread.NORM_PRIORITY;
			shipWindowMs = 100;
			sendDelayMs = 300;
			compressionLevel = QpApplication.DEFAULT_COMPRESSION_LEVEL;
			recoveryMode = RecoveryMode.ABORT;
			createSpecialRecoveryRouter = true;
			bufferReceivedTuples = false;
			maxBufferedLength = 128 * 1024;
			maxBufferedWaitMs = 100;
		}
		
		private static final RecoveryMode modes[] = RecoveryMode.values();
		
		public void serialize(OutputBuffer buf) {
			buf.writeBoolean(discardResults);
			buf.writeBoolean(multipleScanThreads);
			buf.writeInt(threadsPerDistributedScan);
			buf.writeInt(scanThreadPriority);
			buf.writeInt(shipWindowMs);
			buf.writeInt(sendDelayMs);
			buf.writeInt(compressionLevel);
			buf.writeInt(recoveryMode.ordinal());
			buf.writeBoolean(createSpecialRecoveryRouter);
			buf.writeBoolean(bufferReceivedTuples);
			buf.writeInt(maxBufferedLength);
			buf.writeInt(maxBufferedWaitMs);
		}
		
		private Configuration(boolean discardResults, boolean multipleScanThreads, int threadsPerDistributedScan, int scanThreadPriority,
				int shipWindowMs, int sendDelayMs, int compressionLevel, RecoveryMode recoveryMode, boolean createSpecialRecoveryRouter, boolean bufferReceivedTuples, int maxBufferedLength, int maxBufferedWaitMs) {
			this.discardResults = discardResults;
			this.multipleScanThreads = multipleScanThreads;
			this.threadsPerDistributedScan = threadsPerDistributedScan;
			this.scanThreadPriority = scanThreadPriority;
			this.shipWindowMs = shipWindowMs;
			this.sendDelayMs = sendDelayMs;
			this.compressionLevel = compressionLevel;
			this.recoveryMode = recoveryMode;
			this.createSpecialRecoveryRouter = createSpecialRecoveryRouter;
			this.bufferReceivedTuples = bufferReceivedTuples;
			this.maxBufferedLength = maxBufferedLength;
			this.maxBufferedWaitMs = maxBufferedWaitMs;
		}
		
		public static Configuration deserialize(InputBuffer buf) {
			boolean discardResults = buf.readBoolean();
			boolean multipleScanThreads = buf.readBoolean();
			int threadsPerDistributedScan = buf.readInt();
			int scanThreadPriority = buf.readInt();
			int shipWindowMs = buf.readInt();
			int sendDelayMs = buf.readInt();
			int compressionLevel = buf.readInt();
			RecoveryMode recoveryMode = modes[buf.readInt()];
			boolean createSpecialRecoveryRouter = buf.readBoolean();
			boolean bufferReceivedTuples = buf.readBoolean();
			int maxBufferedLength = buf.readInt();
			int maxBufferedWaitMs = buf.readInt();
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration changeScanPriority(int newScanThreadPriority) {
			if (newScanThreadPriority < Thread.MIN_PRIORITY || newScanThreadPriority > Thread.MAX_PRIORITY) {
				throw new IllegalArgumentException();
			}
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, newScanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration setSequentialScans() {
			return new Configuration(discardResults, false, 1, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration setMultipleScans(int numThreadsPerDistributedScan) {
			return new Configuration(discardResults, true, numThreadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration setNoShipBatching() {
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					Integer.MIN_VALUE, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration setShipBatching(int newShipWindowMs) {
			if (newShipWindowMs <= 0) {
				throw new IllegalArgumentException();
			}
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					newShipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration setDiscardResults(boolean newDiscardResults) {
			return new Configuration(newDiscardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}

		public Configuration setRecoveryMode(RecoveryMode newRecoveryMode) {
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, newRecoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}
		
		public Configuration setCompressionLevel(int newCompressionLevel) {
			if (newCompressionLevel < -1 || newCompressionLevel > Deflater.BEST_COMPRESSION) {
				throw new IllegalArgumentException();
			}
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, newCompressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}
		
		public Configuration setCreateSpecialRecoveryRouter(boolean createSpecialRecoveryRouter) {
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}
		
		public Configuration setBufferReceivedTuples(boolean bufferReceivedTuples) {
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}
		
		public Configuration setMaxBufferedLength(int maxBufferedLength) {
			if (maxBufferedLength <= 0) {
				throw new IllegalArgumentException();
			}
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);
		}
		
		public Configuration setMaxBufferedWaitMs(int maxBufferedWaitMs) {
			return new Configuration(discardResults, multipleScanThreads, threadsPerDistributedScan, scanThreadPriority,
					shipWindowMs, sendDelayMs, compressionLevel, recoveryMode, createSpecialRecoveryRouter, bufferReceivedTuples, maxBufferedLength, maxBufferedWaitMs);			
		}
	}

	private void storeSentMessage(QpMessage m) {
		if (! m.retryable()) {
			return;
		}
		pendingMessages.put(m.messageId, this.messageSerialization.serialize(m));
	}

	private QpMessage retrieveSentMessage(long msgId) throws SerializationException {
		byte[] data = pendingMessages.get(msgId);
		if (data == null) {
			return null;
		}
		return messageSerialization.deserialize(data, this.localAddr);
	}

	private void removeSentMessage(long msgId) {
		pendingMessages.remove(msgId);
	}
}
