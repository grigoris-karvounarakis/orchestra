package edu.upenn.cis.orchestra.p2pqp;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.Channel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.FileChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.apache.log4j.Logger;

import edu.upenn.cis.orchestra.p2pqp.QpMessage.Priority;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.InetSocketAddressCache;
import edu.upenn.cis.orchestra.util.IntList;
import edu.upenn.cis.orchestra.util.LongList;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.RotatingSet;

public class SocketManager {
	public static boolean usePings = true;
	static final int MAX_SPILL_FILE_SIZE = 2 * 1024 * 1024;
	static final int MAX_BYTES_TO_WRITE_FROM_FILE = 256 * 1024;
	static final int DEFAULT_PING_SEND_INTERVAL_MS = 250;
	static final int DEFAULT_PING_TIMEOUT_MS = 10000;
	public static int DEFAULT_MSG_BUFFER_SIZE = 256 * 1024;
	static final int DEFAULT_REPLY_BUFFER_SIZE = 2 * 1024;
	public static int DEFAULT_MAX_MEMORY_BUFFERS_PER_SOCKET = 32;
	static final int CHECK_SEND_INTERVAL_MS = 500;
	static final int CONNECT_TIMEOUT_MS = 5000;
	public static int MAX_DIRECT_BUFFERS = 256;
	final InetSocketAddress bindAddress;
	final InetSocketAddress publicAddress;
	private Selector selector;
	private SelectorThread st;
	private DeleteThread dt;
	private WriteFilesThread wft;
	private CheckPingsThread cpt;
	private MessagesStatusThread mst;
	private ShowStatusThread sst;
	private ThrottlingStatusThread tst;
	private ConnectionReceivedThread crt;
	private final SocketManagerClient client;
	private static final Logger logger = Logger.getLogger(SocketManager.class);
	private final int msgBufferSize;
	private final int replyBufferSize;
	private final int maxMemoryBuffersPerSocket;
	private final int pingSendIntervalMs = DEFAULT_PING_SEND_INTERVAL_MS;
	private final int pingCheckIntervalMs = DEFAULT_PING_SEND_INTERVAL_MS / 2;
	private final int pingTimeoutMs = DEFAULT_PING_TIMEOUT_MS;
	private final ScratchFileGenerator sfg;
	private final QpMessageSerialization qms;
	private PingsSelectorThread pst;
	private final ServerSocketChannel server;
	
	static final int RECONNECT_INTERVAL_MS = 60 * 1000; // 60 sec

	private final ArrayDeque<InetSocketAddress> nodesToPing = new ArrayDeque<InetSocketAddress>();
	private final Queue<InetSocketAddress> nodesHaveBeenPinged = new ArrayDeque<InetSocketAddress>();

	// This set is only modified by the SocketManagerThread while
	// it is running. It contains all incoming and outgoing channels
	private final Set<Channel> channels = new HashSet<Channel>();

	private final Set<WritingConnectionManager> writingConnectionsHaveWrittenOrRead = new HashSet<WritingConnectionManager>();
	private final List<RotatingSet<ReadingConnectionManager>> readingConnectionsHaveRead = new ArrayList<RotatingSet<ReadingConnectionManager>>(prios.length);

	private static final QpMessage.Priority[] prios = QpMessage.Priority.values();

	private final Set<InetSocketAddress> receivedIncomingConnections = new HashSet<InetSocketAddress>();

	private static final InetSocketAddressCache cache = new InetSocketAddressCache();
	
	private static class AddrAndPrio {
		final InetSocketAddress addr;
		final QpMessage.Priority prio;

		AddrAndPrio(InetSocketAddress addr, QpMessage.Priority prio) {
			this.addr = addr;
			this.prio = prio;
		}

		public int hashCode() {
			return addr.hashCode() + 37 * prio.hashCode();
		}

		public boolean equals(Object o) {
			AddrAndPrio aap = (AddrAndPrio) o;
			return aap.prio.equals(prio) && aap.addr.equals(addr);
		}

		public String toString() {
			return addr + "," + prio;
		}
	}


	// outgoing is the lock for these members
	private Map<AddrAndPrio,WritingConnectionManager> outgoing =
		new HashMap<AddrAndPrio,WritingConnectionManager>();
	private Set<AddrAndPrio> opening = new HashSet<AddrAndPrio>();
	private Map<AddrAndPrio,SocketChannel> toRegister = new HashMap<AddrAndPrio,SocketChannel>();

	private Set<ReadingConnectionManager> incoming = new HashSet<ReadingConnectionManager>();

	private volatile boolean closed = false;

	// Map from dead nodes to time when they were determined to be dead
	private final Map<InetSocketAddress,Long> deadNodes = new HashMap<InetSocketAddress,Long>();

	private final Set<InetSocketAddress> throttledNodes = Collections.synchronizedSet(new HashSet<InetSocketAddress>());
	private final Set<InetSocketAddress> throttledBy = Collections.synchronizedSet(new HashSet<InetSocketAddress>());
	private final Set<InetSocketAddress> stoppedThrottling = new HashSet<InetSocketAddress>();
	private final Set<InetSocketAddress> startedThrottling = new HashSet<InetSocketAddress>();
	private final ThreadGroup tg;


	private long totalBytesSent = 0L;

	SocketManager(QpMessageSerialization qms, ScratchFileGenerator sfg, SocketManagerClient client, ThreadGroup tg, int port) throws IOException {
		this(qms, sfg, client, tg, new InetSocketAddress(InetAddress.getLocalHost(), port), null);
	}

	SocketManager(QpMessageSerialization qms, ScratchFileGenerator sfg, SocketManagerClient client, ThreadGroup tg, InetSocketAddress bindAddress, InetSocketAddress publicAddress) throws IOException {
		this(qms, sfg, client, tg, bindAddress, publicAddress, DEFAULT_MSG_BUFFER_SIZE, DEFAULT_REPLY_BUFFER_SIZE, DEFAULT_MAX_MEMORY_BUFFERS_PER_SOCKET);
	}

	SocketManager(QpMessageSerialization qms, ScratchFileGenerator sfg, SocketManagerClient client, ThreadGroup tg, InetSocketAddress bindAddress, InetSocketAddress publicAddress, int msgBufferSize, int replyBufferSize, int maxMemoryBuffersPerSocket) throws IOException {
		if (maxMemoryBuffersPerSocket < 2) {
			throw new IllegalArgumentException("maxMemoryBuffersPerSocket must be at least 2");
		}

		for (int i = 0; i < prios.length; ++i) {
			readingConnectionsHaveRead.add(new RotatingSet<ReadingConnectionManager>());
		}

		this.qms = qms;
		this.sfg = sfg;
		this.bindAddress = bindAddress;
		this.publicAddress = publicAddress;
		this.client = client;
		server = ServerSocketChannel.open();
		server.configureBlocking(false);
		server.socket().bind(bindAddress);
		selector = Selector.open();
		server.register(selector, SelectionKey.OP_ACCEPT);
		this.msgBufferSize = msgBufferSize;
		this.replyBufferSize = replyBufferSize;
		this.maxMemoryBuffersPerSocket = maxMemoryBuffersPerSocket;
		this.tg = tg;

		startThreads();
	}

	private void startThreads() {
		st = new SelectorThread(tg);
		st.start();
		dt = new DeleteThread(tg);
		dt.start();
		wft = new WriteFilesThread(tg);
		wft.start();
		if (usePings) {
			cpt = new CheckPingsThread(tg);
			cpt.start();
			pst = new PingsSelectorThread(tg);
			pst.start();
		}
		mst = new MessagesStatusThread(tg);
		mst.start();
		sst = new ShowStatusThread(tg);
		sst.start();
		tst = new ThrottlingStatusThread(tg);
		tst.start();
		crt = new ConnectionReceivedThread(tg);
		crt.start();
	}

	private void stopThreads() throws InterruptedException {
		st.interrupt();
		st.join();
		dt.interrupt();
		dt.join();
		wft.interrupt();
		wft.join();
		if (cpt != null) {
			cpt.interrupt();
			cpt.join();
		}
		mst.interrupt();
		mst.join();
		if (pst != null) {
			pst.interrupt();
			pst.join();
		}
		sst.interrupt();
		sst.join();
		tst.interrupt();
		tst.join();
		crt.interrupt();
		crt.join();
	}

	void close() throws InterruptedException, IOException {
		if (closed) {
			return;
		}
		closed = true;
		stopThreads();
		selector.close();

		for (File f : toDelete) {
			f.delete();
		}
		toDelete.clear();

		server.close();
		for (Channel c : channels) {
			if (c.isOpen()) {
				c.close();
			}
		}
		channels.clear();
	}

	private WritingConnectionManager getWritingConnection(AddrAndPrio dest) throws InterruptedException {
		Long deadTime;
		synchronized (outgoing) {
			deadTime = deadNodes.get(dest.addr); 
		}
		if (deadTime != null && System.currentTimeMillis() - deadTime < RECONNECT_INTERVAL_MS) {
			return null;
		}
		WritingConnectionManager retval;
		synchronized (outgoing) {
			retval = outgoing.get(dest);
			if (retval == null) {
				if (opening.contains(dest)) {
					while (opening.contains(dest)) {
						outgoing.wait();
					}
					retval = outgoing.get(dest);
					return retval;
				} else {
					opening.add(dest);
				}
			} else {
				return retval;
			}
		}

		logger.info(bindAddress + " is opening connection to " + dest);

		SocketChannel sc = null;
		try {
			sc = SocketChannel.open();
			sc.socket().connect(dest.addr, CONNECT_TIMEOUT_MS);
			sc.configureBlocking(false);
			client.peerIsNotDead(dest.addr);
		} catch (IOException e) {
			logger.error(bindAddress + " had error connecting to " + dest, e);
			sc = null;
		}

		if (sc == null) {
			synchronized (outgoing) {
				deadNodes.put(dest.addr, System.currentTimeMillis());
				opening.remove(dest);
				outgoing.notifyAll();
			}
			return null;
		}

		synchronized (outgoing) {
			deadNodes.remove(dest.addr);
			toRegister.put(dest, sc);
			selector.wakeup();
			while (toRegister.containsKey(dest)) {
				outgoing.wait();
			}
			opening.remove(dest);
			outgoing.notifyAll();
			return outgoing.get(dest);
		}
	}

	void sendMessage(QpMessage m) throws InterruptedException {
		if (closed) {
			return;
		}
		WritingConnectionManager wcm = getWritingConnection(new AddrAndPrio(m.getDest(), m.getPriority()));
		if (wcm == null || wcm.writeClosed()) {
			LongList failed = new LongList();
			failed.add(m.messageId);
			client.messageSendingFailed(m.getDest(), failed);
			if (! closed) {
				client.peerIsDead(m.getDest());
			}
			return;
		}
		WritingConnectionManager.BufferWriter buf = wcm.getOutputBuffer();
		try {
			// Could potentially serialize multiple messages here
			qms.serialize(buf, m);
		} catch (RuntimeException e) {
			logger.error("Error serializing message", e);
			throw e;
		} catch (Exception e) {
			logger.error("Error serializing message", e);
		} finally {
			if (buf.transactionInProgress()) {
				throw new IllegalStateException("Transaction is still in progress after serializing " + m);
			}
		}
		if (logger.isTraceEnabled()) {
			logger.trace("Enqueing message " + m.messageId + " from " + bindAddress + " to " + m.getDest());
		}
		wcm.sendWriteBuffer(buf);
	}

	float getSendingBacklog(InetSocketAddress isa) {
		WritingConnectionManager wcm;
		synchronized (outgoing) {
			wcm = outgoing.get(isa);
		}
		if (wcm == null) {
			return Float.NaN;
		}
		return wcm.estimateSendingBacklog();
	}

	private Queue<QpMessage> incomingMessages = new ArrayDeque<QpMessage>();

	void deliverMessage(QpMessage m) {
		synchronized (readingConnectionsHaveRead) {
			if (incomingMessages.isEmpty()) {
				readingConnectionsHaveRead.notify();
			}
			incomingMessages.add(m);
		}
	}

	QpMessage readMessage() throws InterruptedException {
		for ( ; ; ) {
			ReadingConnectionManager rcm = null;
			synchronized (readingConnectionsHaveRead) {
				if (! incomingMessages.isEmpty()) {
					return incomingMessages.remove();
				}
				for (RotatingSet<ReadingConnectionManager> rs : readingConnectionsHaveRead) {
					if (! rs.isEmpty()) {
						rcm = rs.next();
						break;
					}
				}
				if (rcm == null) {
					readingConnectionsHaveRead.wait();
					continue;
				}
			}
			QpMessage received = null;
			if (rcm != null) {
				received = rcm.readMessage();
			}
			if (received != null) {
				synchronized (processedCount) {
					++processedCount[received.getPriority().ordinal()];
				}
				return received;
			}
		}
	}

	synchronized long getTotalBytesSent() {
		return totalBytesSent;
	}

	private int[] processedCount = new int[prios.length];

	private class ShowStatusThread extends Thread {
		ShowStatusThread(ThreadGroup tg) {
			super(tg, bindAddress + " ShowStatusThread");
		}

		public void run() {
			try {
				int[] processedCountCopy = new int[prios.length];
				while (! isInterrupted()) {
					final long lastTime = System.currentTimeMillis();
					sleep(5000);

					List<ConnectionManager> cms = new ArrayList<ConnectionManager>();

					synchronized (incoming) {
						cms.addAll(incoming);
					}

					synchronized (outgoing) {
						cms.addAll(outgoing.values());
					}

					synchronized (processedCount) {
						for (int i = 0; i < prios.length; ++i) {
							processedCountCopy[i] = processedCount[i];
							processedCount[i] = 0;
						}
					}

					final long currentTime = System.currentTimeMillis();
					final long diff = currentTime - lastTime;
					for (ConnectionManager cm : cms) {
						cm.updateSendingRate((int) diff);
					}

					if (logger.isInfoEnabled()) {
						int sendRate[] = new int[prios.length];
						long enqueuedLength[] = new long[prios.length];
						for (ConnectionManager cm : cms) {
							Priority prio = cm.getPriority();
							if (prio != null) {
								sendRate[prio.ordinal()] += cm.getObservedSendingRate();
								enqueuedLength[prio.ordinal()] += cm.bytesInBuffers();
							}
						}
						StringBuilder sb = new StringBuilder("Sending rate from " + bindAddress + ": ");
						for (int i = 0; i < prios.length; ++i) {
							if (i != 0) {
								sb.append(", ");
							}
							sb.append(prios[i] + " at " + sendRate[i] + " bytes/sec");
						}
						logger.info(sb.toString());
						sb.setLength(0);
						sb.append("Buffer length at " + bindAddress + ": ");
						for (int i = 0; i < prios.length; ++i) {
							if (i != 0) {
								sb.append(", ");
							}
							sb.append(prios[i] + " at " + enqueuedLength[i] + " bytes");
						}
						logger.info(sb.toString());

						sb.setLength(0);
						sb.append("Processed at " + bindAddress + ": ");
						for (int i = 0; i < prios.length; ++i) {
							if (i != 0) {
								sb.append(", ");
							}
							sb.append(processedCountCopy[i] + " " + prios[i]);
						}
						logger.info(sb.toString());
					}


					if (logger.isTraceEnabled()) {
						for (ConnectionManager cm : cms) {
							cm.logStatus();
						}
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}
	}

	private class PingsSelectorThread extends Thread {
		private Selector pingsSelector = null;
		PingsSelectorThread(ThreadGroup tg) {
			super(tg, bindAddress.toString() + " PingsSelectorThread");
			this.setPriority(MAX_PRIORITY);
		}

		public void run() {
			long lastSendTime = 0;

			final ByteBuffer oneByte = ByteBuffer.allocateDirect(1);
			oneByte.put((byte) 0);
			oneByte.position(0);
			oneByte.limit(1);

			DatagramChannel sendAndReceivePings = null;
			SelectionKey sendAndReceivePingsKey = null;

			try {
				pingsSelector = Selector.open();
				lastSendTime = System.currentTimeMillis();
				sendAndReceivePings = DatagramChannel.open();
				sendAndReceivePings.configureBlocking(false);
				DatagramSocket ds = sendAndReceivePings.socket();
				ds.bind(bindAddress);
				sendAndReceivePingsKey = sendAndReceivePings.register(pingsSelector, SelectionKey.OP_READ);
				while (! isInterrupted()) {
					long waitTime = pingSendIntervalMs - (System.currentTimeMillis() - lastSendTime);
					if (waitTime > 0) {
						Thread.sleep(waitTime);
					}
					try {
						pingsSelector.selectNow();
					} catch (ClosedSelectorException cse) {
						return;
					} catch (IOException e) {
						logger.fatal("Error in selector thread for " + bindAddress, e);
						return;
					}

					if (isInterrupted()) {
						return;
					}

					final long time = System.currentTimeMillis();
					if (time - lastSendTime > pingSendIntervalMs) {
						lastSendTime = time;
						synchronized (nodesToPing) {
							if (! nodesHaveBeenPinged.isEmpty()) {
								nodesToPing.addAll(nodesHaveBeenPinged);
								nodesHaveBeenPinged.clear();
								if ((sendAndReceivePingsKey.interestOps() & SelectionKey.OP_WRITE) == 0) {
									sendAndReceivePingsKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
								}
							}
						}
					}

					Iterator<SelectionKey> keys = pingsSelector.selectedKeys().iterator();

					while (keys.hasNext()) {
						SelectionKey key = keys.next();
						keys.remove();

						final DatagramChannel dc = (DatagramChannel) key.channel();
						if (key.isReadable()) {
							for ( ; ; ) {
								oneByte.position(0);
								oneByte.limit(1);
								InetSocketAddress origin = (InetSocketAddress) dc.receive(oneByte);
								if (origin == null) {
									break;
								}
								WritingConnectionManager wcm;
								synchronized (outgoing) {
									wcm = outgoing.get(origin);
								}
								if (wcm == null) {
									continue;
								}
								wcm.lastPingReceived = time;
							}
						}

						if (key.isWritable()) {
							synchronized (nodesToPing) {
								for ( ; ;) {
									InetSocketAddress toPing;
									toPing = nodesToPing.peek();
									if (toPing == null) {
										key.interestOps(SelectionKey.OP_READ);
										break;
									}
									oneByte.position(0);
									oneByte.limit(1);
									int written =  dc.send(oneByte, toPing);
									if (written == 0) {
										break;
									}
									nodesToPing.removeFirst();
									nodesHaveBeenPinged.add(toPing);
								}
							}
						}
					}

				}
			} catch (IOException e) {
				logger.error("Error in PingsSelectorThread", e);
			} catch (InterruptedException e) {
			} finally {
				if (sendAndReceivePingsKey != null) {
					sendAndReceivePingsKey.cancel();
				}
				if (sendAndReceivePings != null) {
					try {
						sendAndReceivePings.close();
					} catch (IOException e) {
						logger.error("Error closing pings DatagramChannel", e);
					}
				}
				if (pingsSelector != null) {
					try {
						pingsSelector.close();
					} catch (IOException e) {
						logger.error("Error closing pings Selector", e);
					}
				}
			}
		}

		public void interrupt() {
			super.interrupt();
			pingsSelector.wakeup();
		}

	}

	private class SelectorThread extends Thread {
		SelectorThread(ThreadGroup tg) {
			super(tg, bindAddress.toString() + " SelectorThread");
			this.setPriority(MAX_PRIORITY);
		}

		public void run() {
			try {
				while (! isInterrupted()) {
					try {
						selector.select();
					} catch (IOException e) {
						logger.fatal("Error in selector thread for " + bindAddress, e);
						return;
					}

					if (isInterrupted()) {
						return;
					}
					synchronized (outgoing) {
						if (! toRegister.isEmpty()) {
							for (Map.Entry<AddrAndPrio, SocketChannel> me : toRegister.entrySet()) {
								AddrAndPrio dest = me.getKey();
								SocketChannel sc = me.getValue();
								try {
									SelectionKey sk = sc.register(selector, SelectionKey.OP_READ);
									// We managed to open a connection
									channels.add(sc);
									WritingConnectionManager wcm = new WritingConnectionManager(dest, sk);
									sk.attach(wcm);
									outgoing.put(dest, wcm);
								} catch (ClosedChannelException e) {
								}
							}
							toRegister.clear();
							outgoing.notifyAll();
						}
					}


					Iterator<SelectionKey> keys = selector.selectedKeys().iterator();

					while (keys.hasNext()) {
						SelectionKey key = keys.next();
						keys.remove();

						try {
							if (key.isAcceptable()) {
								// We have a new incoming connection
								try {
									ServerSocketChannel server = (ServerSocketChannel) key.channel();
									SocketChannel incoming = server.accept();
									channels.add(incoming);
									incoming.configureBlocking(false);
									SelectionKey sk = incoming.register(selector, SelectionKey.OP_READ);
									ReadingConnectionManager cm = new ReadingConnectionManager(sk,incoming.socket().getInetAddress().equals(bindAddress.getAddress()));
									sk.attach(cm);
									synchronized (SocketManager.this.incoming) {
										SocketManager.this.incoming.add(cm);
									}
								} catch (IOException e) {
									logger.error("Error accepting incoming connection", e);
								}
								continue;
							}

							ConnectionManager cm = (ConnectionManager) key.attachment();
							SocketChannel sc = (SocketChannel) key.channel();
							if (key.isReadable()) {
								try {
									cm.doRead(sc);
								} catch (IOException e) {
									logger.error(bindAddress + " had error reading from " + sc.socket().getRemoteSocketAddress() + ", closing connection", e);
									cm.close();
									continue;
								}
							}
							if (key.isWritable()) {
								try {
									cm.doWrite(sc);
								} catch (IOException e) {
									logger.error(bindAddress + " had error writing to " + sc.socket().getRemoteSocketAddress() + ", closing connection", e);
									cm.close();
									continue;
								}
							}
						} catch (CancelledKeyException cke) {
							continue;
						}
					}
				}
			} catch (InterruptedException ie) {
			} catch (Exception e) {
				logger.fatal("Error in SocketManager", e);
			}
		}

		public void interrupt() {
			super.interrupt();
			selector.wakeup();
		}
	}

	private class WriteFilesThread extends Thread {
		WriteFilesThread(ThreadGroup tg) {
			super(tg, bindAddress.toString() + " WriteFilesThread");
		}

		public void run() {
			try {
				while (! isInterrupted()) {
					final List<ConnectionManager> cms;
					synchronized (outgoing) {
						cms = new ArrayList<ConnectionManager>(outgoing.values());
					}
					synchronized (incoming) {
						cms.addAll(incoming);
					}

					int allWritten = 0;
					for (ConnectionManager cm : cms) {
						int buffersWritten = cm.writeFiles();
						if (buffersWritten > 0 && logger.isInfoEnabled()) {
							logger.info("Wrote " + buffersWritten + " buffers to disk for " + cm);
						}
						allWritten += buffersWritten;
					}

					if (allWritten > 0) {
						selector.wakeup();
					}

					Thread.sleep(1000);
				}
			} catch (InterruptedException ie) {
				return;
			} catch (IOException e) {
				logger.error("Error writing files", e);
			}
		}

	}

	private class CheckPingsThread extends Thread {
		CheckPingsThread(ThreadGroup tg) {
			super(tg, bindAddress.toString() + " CheckPingsThread");
			setPriority(Thread.MAX_PRIORITY);
		}

		public void run() {
			try {
				final List<WritingConnectionManager> wcms = new ArrayList<WritingConnectionManager>();
				while (! isInterrupted()) {
					sleep(pingCheckIntervalMs);
					wcms.clear();
					synchronized (outgoing) {
						wcms.addAll(outgoing.values());
					}

					Iterator<WritingConnectionManager> it = wcms.iterator();
					final long currTime = System.currentTimeMillis();
					while (it.hasNext()) {
						WritingConnectionManager wcm = it.next();
						long delay = currTime - wcm.lastPingReceived;
						if (wcm.lastPingReceived < 0 || delay < pingTimeoutMs) {
							it.remove();
						} else if (logger.isInfoEnabled()) {
							logger.info("Connection from " + bindAddress + " to " + wcm.dest + " timed out, delay = " + delay);							
						}
					}

					for (WritingConnectionManager wcm : wcms) {
						try {
							wcm.close();
						} catch (IOException ioe) {
							logger.error("Error closing connection to " + wcm.dest, ioe);
						}
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}

	}

	private class MessagesStatusThread extends Thread {
		MessagesStatusThread(ThreadGroup tg) {
			super(tg, bindAddress.toString() + " MessagesStatusThread");
		}

		public void run() {
			try {
				List<WritingConnectionManager> toProcess = new ArrayList<WritingConnectionManager>();
				while (! isInterrupted()) {
					synchronized (writingConnectionsHaveWrittenOrRead) {
						while (writingConnectionsHaveWrittenOrRead.isEmpty()) {
							writingConnectionsHaveWrittenOrRead.wait();
						}
						toProcess.addAll(writingConnectionsHaveWrittenOrRead);
						writingConnectionsHaveWrittenOrRead.clear();
					}

					for (WritingConnectionManager wcm : toProcess) {
						wcm.sendMessagesSent();
						wcm.sendMessagesReceived();
					}

					toProcess.clear();
				}
			} catch (InterruptedException ie) {
				return;
			}
		}
	}

	private class ThrottlingStatusThread extends Thread {
		ThrottlingStatusThread(ThreadGroup tg) {
			super(tg, bindAddress + " ThrottlingStatusThread");
		}

		public void run() {

			try {
				while (! isInterrupted()) {
					ArrayList<InetSocketAddress> started, stopped;
					synchronized (startedThrottling) {
						while (startedThrottling.isEmpty() && stoppedThrottling.isEmpty()) {
							startedThrottling.wait();
						}
						started = new ArrayList<InetSocketAddress>(startedThrottling);
						stopped = new ArrayList<InetSocketAddress>(stoppedThrottling);
						startedThrottling.clear();
						stoppedThrottling.clear();
					}
					for (InetSocketAddress node : started) {
						client.startedThrottling(node);
					}
					for (InetSocketAddress node : stopped) {
						client.stoppedThrottling(node);
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}
	}
	
	private class ConnectionReceivedThread extends Thread {
		ConnectionReceivedThread(ThreadGroup tg) {
			super(tg, bindAddress + " ConnectionReceivedThread");
		}
		
		public void run() {
			try {
				while (! isInterrupted()) {
					ArrayList<InetSocketAddress> received;
					synchronized (receivedIncomingConnections) {
						while (receivedIncomingConnections.isEmpty()) {
							receivedIncomingConnections.wait();
						}
						received = new ArrayList<InetSocketAddress>(receivedIncomingConnections);
						receivedIncomingConnections.clear();
					}
					for (InetSocketAddress peer : received) {
						client.peerIsNotDead(peer);
					}
				}
			} catch (InterruptedException ie) {
				return;
			}
		}
	}

	private static final Map<Integer,Deque<ByteBuffer>> freeMsgBuffers = new HashMap<Integer,Deque<ByteBuffer>>();
	private static int numDirectBuffersIssued = 0;

	private ByteBuffer getBuffer(int size) {
		ByteBuffer bb = null;
		if (size == msgBufferSize || size == replyBufferSize) {
			Deque<ByteBuffer> freeBuffersForSize;
			synchronized (freeMsgBuffers) {
				freeBuffersForSize = freeMsgBuffers.get(size);
			}
			if (freeBuffersForSize != null) {
				synchronized (freeBuffersForSize) {
					bb = freeBuffersForSize.pollLast();
				}
			}
		}
		if (bb != null) {
			bb.clear();
			return bb;
		} else {
			boolean allocateDirect = (size == msgBufferSize || size == replyBufferSize);
			if (allocateDirect) {
				synchronized (SocketManager.class) {
					if (numDirectBuffersIssued < MAX_DIRECT_BUFFERS) {
						++numDirectBuffersIssued;
					} else {
						allocateDirect = false;
					}
				}
			}
			bb = allocateDirect ? ByteBuffer.allocateDirect(size) : ByteBuffer.allocate(size);
			return bb;
		}
	}

	private void returnBuffer(ByteBuffer bb) {
		if (bb.isDirect()) {
			final int size = bb.capacity();
			if (size == msgBufferSize || size == replyBufferSize) {
				Deque<ByteBuffer> freeBuffersForSize;
				synchronized (freeMsgBuffers) {
					freeBuffersForSize = freeMsgBuffers.get(size);
					if (freeBuffersForSize == null) {
						freeBuffersForSize = new ArrayDeque<ByteBuffer>();
						freeMsgBuffers.put(size, freeBuffersForSize);
					}
				}
				synchronized (freeBuffersForSize) {
					freeBuffersForSize.addLast(bb);
				}
			}
		}
		// Otherwise, just garbage collect it
	}

	private class DeleteThread extends Thread {

		DeleteThread(ThreadGroup tg) {
			super(tg, bindAddress + " DeleteThread");
		}

		public void run() {
			try {
				while (! isInterrupted()) {
					File f;
					synchronized (toDelete) {
						while (toDelete.isEmpty()) {
							toDelete.wait();
						}
						f = toDelete.pollFirst();
					}
					f.delete();
				}
			} catch (InterruptedException ie) {
			}
			return;
		}
	}

	private Deque<File> toDelete = new ArrayDeque<File>();

	interface ReadableBuffer {
		LongList getNewlySentMessageIds() throws IOException;
		LongList getUnsentMessageIds() throws IOException;
		/**
		 * Write the next bytes from the buffer to the channel
		 * 
		 * @param target	The channel to write the bytes to
		 * @param maxBytesToWrite
		 * 					The maximum number of bytes to write,
		 * 					or <code>Long.MAX_VALUE</code> to write as many as possible
		 * @return			The number of bytes actually written
		 * @throws IOException
		 */
		long writeToChannel(WritableByteChannel target, long maxBytesToWrite) throws IOException;
		boolean remainingToRead();
		long numBytesRemainingToRead();
		boolean reading();
		void dispose() throws IOException;
		void startReading() throws IOException;
	}

	class OutputMemoryBuffer extends OutputBuffer implements ReadableBuffer {
		private ByteBuffer[] bufs;
		private final LongList messageIds = new LongList();
		private final IntList messageEnds = new IntList();
		private int readingBuffer = -1;
		private int writingBuffer = 0;
		private final int bufferSize;

		private int writtenLength = 0;
		private int lastIdReturned = -1;

		OutputMemoryBuffer(int bufferSize) {
			bufs = new ByteBuffer[20];
			bufs[0] = getBuffer(bufferSize);
			this.bufferSize = bufferSize;
		}

		public boolean writing() {
			return readingBuffer < 0;
		}

		public boolean reading() {
			return readingBuffer >= 0;
		}

		public void dispose() {
			for (int i = 0; i <= writingBuffer; ++i) {
				if (bufs[i] != null) {
					returnBuffer(bufs[i]);
					bufs[i] = null;
				}
			}
			bufs = null;
			readingBuffer = -1;
			writingBuffer = 0;
		}

		public int numBuffersInUse() {
			if (writing()) {
				return writingBuffer + 1;
			} else {
				return writingBuffer - readingBuffer + 1;
			}
		}

		public int numBytesWritten() {
			if (reading()) {
				throw new IllegalStateException();
			}
			int retval = 0;
			for (int i = 0; i <= writingBuffer; ++i) {
				retval += bufs[i].position();
			}
			return retval;
		}

		public boolean remainingToRead() {
			if (writing()) {
				throw new IllegalStateException("Am currently writing");
			}
			return readingBuffer <= writingBuffer || (readingBuffer == writingBuffer && bufs[readingBuffer] != null);
		}

		public long numBytesRemainingToRead() {
			int startBuffer = readingBuffer < 0 ? 0 : readingBuffer;
			if (bufs[startBuffer] == null) {
				return 0;
			}
			long retval = 0;
			for (int i = startBuffer; i <= writingBuffer; ++i) {
				retval += bufs[i].remaining();
			}
			return retval;
		}


		public void writeBoolean(boolean b) {
			ensureAvailable(1);
			bufs[writingBuffer].put(b ? (byte) 1 : (byte) 0);
		}

		public void writeBytesNoLength(byte[] bytes) {
			writeBytesNoLength(bytes, 0, bytes.length);
		}

		public void writeBytesNoLength(byte[] bytes, int offset, int length) {
			int remainingInCurrBuf = bufs[writingBuffer].remaining();
			try {
				if (remainingInCurrBuf >= length) {
					bufs[writingBuffer].put(bytes, offset, length);
				} else {
					bufs[writingBuffer].put(bytes, offset, remainingInCurrBuf);
					int stillToWrite = length - remainingInCurrBuf;
					ByteBuffer newBuffer = getBuffer(stillToWrite);
					newBuffer.put(bytes, offset + remainingInCurrBuf, stillToWrite);
					++writingBuffer;
					if (writingBuffer >= bufs.length) {
						ByteBuffer[] newBufs = new ByteBuffer[bufs.length * 2];
						System.arraycopy(bufs, 0, newBufs, 0, bufs.length);
						bufs = newBufs;
					}
					bufs[writingBuffer] = newBuffer;
				}
			} catch (BufferOverflowException e) {
				throw new IllegalStateException("Shouldn't get buffer overflow after checking available space", e);
			}
		}

		public void writeShort(short s) {
			ensureAvailable(2);
			bufs[writingBuffer].putShort(s);
		}

		public void writeInt(int i) {
			ensureAvailable(4);
			bufs[writingBuffer].putInt(i);
		}

		public void writeLong(long l) {
			ensureAvailable(8);
			bufs[writingBuffer].putLong(l);
		}

		public long writeToChannel(WritableByteChannel target, long maxBytesToWrite) throws IOException {
			if (writing()) {
				throw new IllegalStateException("Am writing");
			}
			if (maxBytesToWrite < 0) {
				throw new IllegalArgumentException("maxBytesToWrite should always be positive");
			}

			int written = 0;
			while (readingBuffer <= writingBuffer) {
				if ((written + bufs[readingBuffer].remaining()) <= maxBytesToWrite) {
					written += target.write(bufs[readingBuffer]);
					if (bufs[readingBuffer].remaining() == 0) {
						returnBuffer(bufs[readingBuffer]);
						bufs[readingBuffer] = null;
						++readingBuffer;
					} else {
						break;
					}
				} else {
					ByteBuffer slice = bufs[readingBuffer].slice();
					slice.limit((int) (maxBytesToWrite - written));
					int writtenFromSlice = target.write(slice);
					bufs[readingBuffer].position(bufs[readingBuffer].position() + writtenFromSlice);
					written += writtenFromSlice;
					break;
				}
			}
			writtenLength += written;
			return written;
		}

		public void startReading() {
			if (writing()) {
				readingBuffer = 0;
				for (int i = 0; i <= writingBuffer; ++i) {
					bufs[i].flip();
				}
			} else {
				throw new IllegalStateException("Buffer is already reading");
			}
		}

		public int getWritingPos() {
			if (! writing()) {
				throw new IllegalStateException("Am not writing");
			}
			int pos = 0;
			for (int i = 0; i <= writingBuffer; ++i) {
				pos += bufs[i].position();
			}
			return pos;
		}

		public void writeInt(int val, int pos) {
			for (int i = 0; i <= writingBuffer; ++i) {
				ByteBuffer buf = bufs[i];
				int bufPos = buf.position();
				if (pos <= bufPos) {
					buf.putInt(pos, val);
					return;
				} else {
					pos -= bufPos;
				}
			}

			throw new IllegalArgumentException("pos exceeds buffer length");
		}

		public void addMessageId(long msgId) {
			if (reading()) {
				throw new IllegalStateException("Am reading");
			}
			messageIds.add(msgId);
			messageEnds.add(getWritingPos());
		}

		public LongList getNewlySentMessageIds() throws IOException {
			if (! reading()) {
				throw new IllegalStateException("Am writing");
			}
			int returnCount = 0;
			while (lastIdReturned + 1 + returnCount < messageEnds.size() && messageEnds.get(lastIdReturned + 1 + returnCount) <= writtenLength) {
				++returnCount;
			}
			if (returnCount != 0) {
				LongList retval = new LongList();
				retval.addFrom(messageIds, lastIdReturned + 1, returnCount);
				lastIdReturned += returnCount;
				return retval;
			} else {
				return new LongList(0);
			}
		}

		@Override
		public LongList getUnsentMessageIds() throws IOException {
			if (! reading()) {
				throw new IllegalStateException("Am writing");
			}
			LongList retval = new LongList();
			retval.addFrom(messageIds, lastIdReturned + 1, messageIds.size() - (lastIdReturned + 1));
			return retval;
		}

		IntList getMessageEnds() {
			if (writing()) {
				throw new IllegalStateException("Am writing");
			}
			return messageEnds;
		}

		protected void ensureAvailable(int numBytes) {
			if (reading()) {
				throw new IllegalStateException("Am reading");
			}
			if (bufs[writingBuffer].remaining() < numBytes) {
				++writingBuffer;
				if (writingBuffer >= bufs.length) {
					ByteBuffer[] newBufs = new ByteBuffer[bufs.length * 2];
					System.arraycopy(bufs, 0, newBufs, 0, bufs.length);
					bufs = newBufs;
				}
				bufs[writingBuffer] = getBuffer(bufferSize);
			}
		}

		protected void setWritingPos(int pos) {
			int soFar = 0;
			for (int i = 0; i < writingBuffer; ++i) {
				if (soFar + bufs[i].position() > pos) {
					int inCurrBuf = bufs[i].position() - soFar;
					bufs[i].position(inCurrBuf);
					for (int j = i + 1; j <= writingBuffer; ++j) {
						returnBuffer(bufs[j]);
						bufs[j] = null;
					}
					writingBuffer = i;
					return;
				} else {
					soFar += bufs[i].position();
				}
			}
			throw new IllegalArgumentException("pos exceeds the length of written data");
		}

		/**
		 * Copy the contents of the supplied MemoryBuffer into this MemoryBuffer, if possible
		 * 
		 * @param mb	The buffer to copy from
		 * @return		<code>true</code> if the contents were copied and <code>mb</code> has been disposed of,
		 * 				<code>false</code> if <code>mb</code> must still be sent separately.
		 */
		boolean readFrom(OutputMemoryBuffer mb) {
			if (mb.writingBuffer == 0 && bufs[writingBuffer].remaining() >= mb.bufs[0].position()) {
				// mb contains only one buffer and we can copy it into the last buffer in this buffer
				this.messageIds.addFrom(mb.messageIds);
				this.messageEnds.addWithShift(mb.messageEnds, this.getWritingPos());
				ByteBuffer buf = mb.bufs[0];
				buf.flip();
				bufs[writingBuffer].put(buf);
				mb.dispose();
				return true;
			} else {
				return false;
			}
		}
	}

	class OutputFileBuffer implements ReadableBuffer {
		private final File fileName;
		private final FileChannel channel;
		private final RandomAccessFile file;
		private boolean writing = true;
		private long pos, remaining;

		private final LongList messageIds = new LongList();
		private final IntList messageEnds = new IntList();
		private int lastIdReturned = -1;

		OutputFileBuffer(File fileName) throws IOException {
			this.fileName = fileName;
			file = new RandomAccessFile(fileName,"rw");
			channel = file.getChannel();
		}

		public long writeToChannel(WritableByteChannel target, long maxBytesToWrite) throws IOException {
			if (writing) {
				throw new IllegalStateException("Am writing");
			}
			if (maxBytesToWrite < 0) {
				throw new IllegalArgumentException("maxBytesToWrite should be positive");
			}
			if (maxBytesToWrite > MAX_BYTES_TO_WRITE_FROM_FILE) {
				maxBytesToWrite = MAX_BYTES_TO_WRITE_FROM_FILE;
			}
			long written;
			try {
				written = channel.transferTo(pos, remaining < maxBytesToWrite ? remaining : maxBytesToWrite, target);
			} catch (IOException e) {
				if (e.getMessage().equals("Resource temporarily unavailable")) {
					// Per http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=5103988
					written = 0;
				} else {
					throw e;
				}
			}
			pos += written;
			remaining -= written;
			return written;
		}

		public boolean remainingToRead() {
			if (writing) {
				throw new IllegalStateException("Am reading");
			}
			return remaining > 0;
		}

		public long numBytesRemainingToRead() {
			if (writing) {
				try {
					return channel.size();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			} else {
				return remaining;
			}
		}

		public void dispose() throws IOException {
			file.close();
			synchronized (toDelete) {
				toDelete.add(fileName);
				toDelete.notify();
			}
		}

		public void startReading() throws IOException {
			if (! writing) {
				throw new IllegalStateException("Am already reading");
			}
			writing = false;
			pos = 0;
			remaining = channel.size();
		}

		public boolean reading() {
			return (! writing);
		}

		void write(OutputMemoryBuffer buf) throws IOException {
			if (! writing) {
				throw new IllegalStateException("Am reading");
			}
			if (! buf.reading()) {
				buf.startReading();
			}
			final int pos = (int) channel.size();
			while (buf.remainingToRead()) {
				buf.writeToChannel(channel, Long.MAX_VALUE);
			}
			messageIds.addFrom(buf.getNewlySentMessageIds());
			messageEnds.addWithShift(buf.getMessageEnds(), pos);
		}

		@Override
		public LongList getNewlySentMessageIds() throws IOException {
			if (! reading()) {
				throw new IllegalStateException("Am writing");
			}
			int pos = (int) channel.position();
			int returnCount = 0;
			while (lastIdReturned + 1 + returnCount < messageEnds.size() && messageEnds.get(lastIdReturned + 1 + returnCount) <= pos) {
				++returnCount;
			}
			if (returnCount != 0) {
				LongList retval = new LongList();
				retval.addFrom(messageIds, lastIdReturned + 1, returnCount);
				lastIdReturned += returnCount;
				return retval;
			} else {
				return new LongList(0);
			}
		}

		@Override
		public LongList getUnsentMessageIds() throws IOException {
			if (! reading()) {
				throw new IllegalStateException("Am writing");
			}
			LongList retval = new LongList();
			retval.addFrom(messageIds, lastIdReturned + 1, messageIds.size() - (lastIdReturned + 1));
			return retval;
		}
	}

	private static class InputMemoryBuffer {
		private final ByteBuffer buf;
		private int readStart;
		private boolean reading;

		InputMemoryBuffer(ByteBuffer buf) {
			this.buf = buf;
			buf.clear();
			readStart = 0;
			reading = false;
		}

		ByteBuffer getReadingBuffer() {
			if (! reading) {
				int writeStart = buf.position();
				reading = true;
				buf.limit(writeStart);
				buf.position(readStart);
			}
			return buf;
		}

		ByteBuffer getWritingBuffer() {
			if (reading) {
				readStart = buf.position();
				int writeStart = buf.limit();
				reading = false;
				buf.limit(buf.capacity());
				buf.position(writeStart);
			}
			return buf;
		}

		int remainingToWrite() {
			if (reading) {
				return buf.capacity() - buf.limit();
			} else {
				return buf.remaining();
			}
		}

		int remainingToRead() {
			if (reading) {
				return buf.remaining();
			} else {
				return buf.position() - readStart;
			}
		}

		void clear() {
			reading = false;
			readStart = 0;
			buf.clear();
		}
	}

	private abstract class ConnectionManager {

		/**
		 * Flushed content that has been pulled from the subclass. It will be written directly from
		 * the file to the output socket
		 */
		private final Deque<OutputFileBuffer> outputFileBuffers;
		/**
		 * The output buffer we're currently sending
		 */
		private ReadableBuffer currentOutputBuffer;
		private boolean currentOutputBufferIsUrgent;

		// If we're currently sending the outputMemoryBuffer, its length is stored here
		private long sendingOutputBufferLength;

		// # of bytes that have been sent since the last call to updateSendingRate
		private long recentBytesSent = 0;
		// # Estimate of how fast we're actually sending data, as computed by updateSendingRate, in bytes/sec
		private int observedSendingRate = 25000; 


		final Deque<InputMemoryBuffer> inputBuffers;
		private volatile boolean writeClosed = false;
		private boolean closed = false;

		private boolean wantsWrite;

		private final SelectionKey sk;

		final int inputBufferLen;

		protected final LongList messagesSent;

		long bytesSent = 0, bytesReceived = 0;
		protected boolean isLocal;

		ConnectionManager(SelectionKey sk, boolean isLocal, int inputBufferLen) throws IOException {
			this(sk, isLocal, inputBufferLen, null);
		}

		/**
		 * Construct a new ConnectionManger
		 * 
		 * @param sk				The selection key associated with the socket
		 * @param isLocal			<code>true</code> if the connection is to the local machine
		 * @param inputBufferLen	The length of the input buffer, in bytes
		 * @param firstOutputBuffer	Content to send over the socket before anything else, or <code>null</code>
		 * @throws IOException 
		 */
		ConnectionManager(SelectionKey sk, boolean isLocal, int inputBufferLen, OutputMemoryBuffer firstOutputBuffer) throws IOException {
			this.sk = sk;
			int ops = sk.interestOps();
			wantsWrite = (ops & SelectionKey.OP_WRITE) != 0;
			outputFileBuffers = new ArrayDeque<OutputFileBuffer>();
			inputBuffers = new ArrayDeque<InputMemoryBuffer>();

			this.isLocal = isLocal;
			this.inputBufferLen = inputBufferLen;
			messagesSent = new LongList();
			currentOutputBuffer = firstOutputBuffer;
			if (currentOutputBuffer != null) {
				currentOutputBuffer.startReading();
				setWantsWrite(true);
			}
		}



		final void doRead(SocketChannel sc) throws IOException, InterruptedException {
			int totalBytesRead = 0;
			try {
				for ( ; ; ) {
					InputMemoryBuffer buf;
					synchronized (this) {
						buf = inputBuffers.pollLast();
						if (buf != null && buf.remainingToWrite() == 0) {
							inputBuffers.addLast(buf);
							buf = null;
						}
					}
					if (buf == null) {
						buf = new InputMemoryBuffer(getBuffer(inputBufferLen));
					}
					boolean hitEnd = false;
					int bytesRead = 0;
					try {
						bytesRead = sc.read(buf.getWritingBuffer());
						if (bytesRead < 0) {
							hitEnd = true;
						}
					} catch (ClosedChannelException cce) {
						hitEnd = true;
					}
					if (hitEnd) {
						close();
						return;
					}
					if (bytesRead >= 0) {
						totalBytesRead += bytesRead;
					}
					boolean bufWasFull = (buf.remainingToWrite() == 0);
					synchronized (this) {
						inputBuffers.addLast(buf);
					}
					if (! bufWasFull) {
						break;
					}
				}
				if (totalBytesRead > 0) {
					hasRead();
				}
			} finally {
				synchronized (this) {
					bytesReceived += totalBytesRead;
				}
			}
		}

		final void doWrite(SocketChannel sc) throws IOException {
			if (writeClosed) {
				return;
			}
			long totalWritten = 0;
			ReadableBuffer rb = null;
			try {
				for ( ; ; ) {
					long written = 0;
					synchronized (this) {
						rb = currentOutputBuffer;
						currentOutputBuffer = null;
						if (rb == null) {
							rb = pullUrgentBuffer();
							currentOutputBufferIsUrgent = true;
						}
						if (rb == null) {
							currentOutputBufferIsUrgent = false;
							rb = outputFileBuffers.pollFirst();
						}
						if (rb == null) {
							rb = pullBuffer();
						}
						if (rb == null) {
							setDoesNotWantWrite();
							return;
						}
						if (! rb.reading()) {
							rb.startReading();
						}
						if (currentOutputBufferIsUrgent) {
							sendingOutputBufferLength = 0;
						} else {
							sendingOutputBufferLength = rb.numBytesRemainingToRead();
						}
					}
					try {
						final long writtenThisTime = rb.writeToChannel(sc, Long.MAX_VALUE);
						if (! currentOutputBufferIsUrgent) {
							written = writtenThisTime;
						}
						LongList msgIds = rb.getNewlySentMessageIds();
						if (msgIds.size() != 0) {
							synchronized (messagesSent) {
								messagesSent.addFrom(msgIds);
							}
						}
						if (rb.remainingToRead()) {
							return;
						} else {
							rb.dispose();
							rb = null;
						}
					} catch (ClosedChannelException cce) {
						writeClosed = true;
						return;
					} finally {
						totalWritten += written;
						synchronized (this) {
							currentOutputBuffer = rb;
							sendingOutputBufferLength = 0;
							recentBytesSent += written;
							bytesSent += written;
						}
					}
				}
			} finally {
				if (totalWritten > 0) {
					hasWritten();
				}
				if (! this.isLocal) {
					synchronized (SocketManager.this) {
						totalBytesSent += totalWritten;
					}
				}
			}
		}

		synchronized void close() throws IOException {
			if (closed) {
				return;
			}
			sk.cancel();
			try {
				if (sk.channel().isOpen()) {
					sk.channel().close();
				}
			} catch (IOException ioe) {
				logger.error("Error closing " + toString());
			}
			wantsWrite = false;
			for (InputMemoryBuffer buf : inputBuffers) {
				returnBuffer(buf.getReadingBuffer());
			}
			inputBuffers.clear();

			List<ReadableBuffer> bufs = new ArrayList<ReadableBuffer>();
			if (currentOutputBuffer != null) {
				bufs.add(currentOutputBuffer);
				currentOutputBuffer = null;
			}
			bufs.addAll(outputFileBuffers);
			outputFileBuffers.clear();
			ReadableBuffer b;
			while ((b = this.pullBuffer()) != null) {
				bufs.add(b);
			}
			// This is a hack
			WritingConnectionManager wcm = null;
			if (this instanceof WritingConnectionManager) {
				wcm = (WritingConnectionManager) this;
			}
			for (ReadableBuffer bb : bufs) {
				if (wcm != null) {
					if (! bb.reading()) {
						bb.startReading();
					}
					client.messageSendingFailed(wcm.dest, bb.getUnsentMessageIds());
				}
				bb.dispose();
			}
			closed = true;
			remove();
		}

		synchronized boolean isClosed() {
			return closed;
		}

		/**
		 * Invoked by the Selector Thread after a non-empty read occurs
		 */
		abstract void hasRead();

		/**
		 * Invoked by the Selector Thread after a non-empty write occurs
		 */
		abstract void hasWritten();

		/**
		 * Remove this connection manager from the SocketManger
		 */
		abstract void remove();

		synchronized void setWantsWrite(boolean wakeup) {
			if (wantsWrite) {
				return;
			}
			wantsWrite = true;
			try {
				sk.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
			} catch (CancelledKeyException cke) {
				return;
			}
			// Otherwise the change may never be seen
			if (wakeup) {
				sk.selector().wakeup();
			}
		}

		synchronized void setDoesNotWantWrite() {
			if (! wantsWrite) {
				return;
			}
			wantsWrite = false;
			try {
				sk.interestOps(SelectionKey.OP_READ);
			} catch (CancelledKeyException cke) {
				return;
			}
		}

		/**
		 * Pull a buffer from a subclass into the ConnectionManager so it can be sent. The buffers
		 * pulled using this method may be sent out of order
		 * 
		 * @return			The buffer to be sent, or <code>null</code> if there is no buffer
		 * 					that needs to be sent
		 */
		abstract OutputMemoryBuffer pullBuffer();

		/**
		 * Pull a buffer from a subclass into the ConnectionManager so it can be sent. The buffers
		 * pulled using this method will be sent in order, and before any buffers from <code>pullBuffer</code>
		 * 
		 * @return			The buffer to be sent, or <code>null</code> if there is no urgent buffer
		 * 					that needs to be sent
		 */
		abstract OutputMemoryBuffer pullUrgentBuffer();


		/**
		 * Get the number of bytes currently in buffers than can be pulled. This will increase,
		 * except when pullBuffer() is called.
		 * 
		 * @return			The number of bytes
		 */
		abstract long bytesCanBePulled();


		/**
		 * Return the number of output byte buffers that the subclass currently holds
		 * 
		 * @return			The number of output buffers
		 */
		abstract int byteBuffersInUse();

		int writeFiles() throws IOException {
			if (byteBuffersInUse() < maxMemoryBuffersPerSocket) {
				return 0;
			}

			try {
				int writtenCount = 0;
				for ( ; ; ) {
					OutputMemoryBuffer toWrite = pullBuffer();
					if (toWrite == null) {
						break;
					}
					OutputFileBuffer fb = null;
					synchronized (this) {
						if (! outputFileBuffers.isEmpty()) {
							fb = outputFileBuffers.removeLast();
							if (fb.reading() || (fb.numBytesRemainingToRead() + toWrite.numBytesRemainingToRead()) >= MAX_SPILL_FILE_SIZE) {
								outputFileBuffers.addLast(fb);
								fb = null;
							}
						}
					}
					if (fb == null) {
						fb = new OutputFileBuffer(sfg.getFile());
					}

					fb.write(toWrite);
					toWrite.dispose();

					++writtenCount;
					synchronized (this) {
						outputFileBuffers.addLast(fb);
						setWantsWrite(false);
					}
				}
				return writtenCount;
			} finally {
			}
		}

		boolean writeClosed() {
			return writeClosed;
		}


		synchronized void updateSendingRate(int interval) {
			int	observedSendingRate;
			observedSendingRate = (int) (1000 * recentBytesSent / interval);
			this.observedSendingRate = observedSendingRate;
			recentBytesSent = 0;
		}

		synchronized long bytesInBuffers() {
			long bytesInBuffers = this.bytesCanBePulled();
			if (this.currentOutputBuffer == null) {
				bytesInBuffers += this.sendingOutputBufferLength;
			} else if (! this.currentOutputBufferIsUrgent) {
				bytesInBuffers += this.currentOutputBuffer.numBytesRemainingToRead();
			}
			for (OutputFileBuffer fb : this.outputFileBuffers) {
				bytesInBuffers += fb.numBytesRemainingToRead();
			}
			return bytesInBuffers;
		}

		synchronized float estimateSendingBacklog() {
			long bytesInBuffers = bytesInBuffers();
			if (bytesInBuffers != 0 && observedSendingRate == 0) {
				return Float.POSITIVE_INFINITY;
			} else {
				return((float) bytesInBuffers) / observedSendingRate;
			}
		}

		synchronized int getObservedSendingRate() {
			return observedSendingRate;
		}


		abstract void logStatus();

		abstract QpMessage.Priority getPriority();
	}

	// Return channel commands, must be negative
	static final int START_THROTTLING = -1, STOP_THROTTLING = -2;

	private class ReadingConnectionManager extends ConnectionManager {
		private int currMsgSize = Integer.MIN_VALUE;

		private long numBytesProcessed = 0;

		private class BufferReader extends TransactionalInputBuffer {
			private ByteBuffer buf;
			private int txnStartPos;
			private boolean ended;
			private int currMsgSize;
			
			BufferReader() {
				super(cache);
			}

			void init(ByteBuffer buf, int currMsgSize) {
				this.buf = buf;
				txnStartPos = buf.position();
				ended = false;
				this.currMsgSize = currMsgSize;
			}

			@Override
			void endReadingTransaction() throws IOException, TransactionSizeException {
				if (ended) {
					throw new IOException("Transaction has already ended");
				}
				int read = buf.position() - txnStartPos;
				if (read != currMsgSize) {
					StringBuilder sb = new StringBuilder("At start of failed transaction, position : " + txnStartPos + ", capacity: " + buf.capacity() + ", limit: " + buf.limit() + ", buffer contents: ");
					sb.append(SocketManager.toString(buf));
					logger.info(sb.toString());
					buf.position(txnStartPos + currMsgSize);
					throw new TransactionSizeException(currMsgSize, read);
				}
				ended = true;
			}

			@Override
			void skipTransaction() throws IOException {
				if (ended) {
					throw new IOException("Transaction has already ended");
				}
				buf.position(txnStartPos + currMsgSize);
				ended = true;				
			}

			@Override
			public
			boolean readBoolean() {
				byte b = buf.get();
				return (b != 0);
			}

			@Override
			public
			byte[] readBytes(int length) {
				byte[] retval = new byte[length];
				buf.get(retval);
				return retval;
			}

			@Override
			public
			short readShort() {
				return buf.getShort();
			}

			@Override
			public
			int readInt() {
				return buf.getInt();
			}

			@Override
			public
			long readLong() {
				return buf.getLong();
			}

			boolean hasEnded() {
				return ended;
			}

			@Override
			public ByteArrayWrapper readByteArrayWrapperWithoutCopying(int length) {
				return new ByteArrayWrapper(readBytes(length));
			}
			
			@Override
			public byte[] readBytesWithoutCopying(int length) {
				this.lastReadOffset = 0;
				return readBytes(length);
			}
		}

		private BufferReader txnInputBuf;

		ReadingConnectionManager(SelectionKey sk, boolean isLocal) throws IOException {
			super(sk,isLocal,msgBufferSize);
			txnInputBuf = new BufferReader();
			idBuf = new OutputMemoryBuffer(replyBufferSize);
			idBuf.writeInt(0);
		}

		private InetSocketAddress origin;
		private int prioOrdinal = -1;
		private ByteBuffer extraInputBuffer = null;
		private OutputMemoryBuffer idBuf;

		private final Object readLock = new Object();

		@Override
		synchronized void close() throws IOException {
			super.close();
			if (idBuf != null) {
				idBuf.dispose();
				idBuf = null;
			}
			// extraInputBuffer is not direct so there's no need to return it
		}

		@Override
		synchronized void hasRead() {
			if (prioOrdinal < 0) {
				InputMemoryBuffer inputBuffer = inputBuffers.pollFirst();
				if (inputBuffer == null) {
					return;
				}
				ByteBuffer buf = inputBuffer.getReadingBuffer();
				if (origin == null && buf.remaining() >= OutputBuffer.inetSocketAddressLen) {
					byte[] originData = new byte[OutputBuffer.inetSocketAddressLen];
					buf.get(originData);
					try {
						origin = cache.probe(originData);
					} catch (UnknownHostException e) {
						logger.error("Error receiving origin", e);
						try {
							close();
						} catch (IOException ioe) {
							logger.error("Error closing connection", ioe);
						}
						return;
					}
					if (origin.equals(bindAddress) || origin.equals(publicAddress)) {
						this.isLocal = true;
					}
				}
				if (prioOrdinal < 0 && buf.remaining() >= 4) {
					prioOrdinal = buf.getInt();
				}
				if (inputBuffer.remainingToRead() == 0) {
					returnBuffer(buf);
				} else {
					inputBuffers.addFirst(inputBuffer);
				}
				if (prioOrdinal < 0) {
					return;
				}
				if (prios[prioOrdinal] == Priority.NORMAL) {
					synchronized (nodesToPing) {
						nodesHaveBeenPinged.add(origin);
					}
				}
				synchronized (receivedIncomingConnections) {
					receivedIncomingConnections.add(origin);
					receivedIncomingConnections.notify();
				}
			}
			if (inputBuffers.size() > maxMemoryBuffersPerSocket || forcedThrottling) {
				throttle();
			}
			synchronized (readingConnectionsHaveRead) {
				if (readingConnectionsHaveRead.get(prioOrdinal).add(this)) {
					readingConnectionsHaveRead.notifyAll();
				}
			}
		}

		synchronized void checkUnthrottle() {
			if (inputBuffers.size() <= (maxMemoryBuffersPerSocket / 2) && (! forcedThrottling)) {
				unthrottle();
			}

		}

		QpMessage readMessage() {
			synchronized (readLock) {
				for ( ; ; ) {
					ByteBuffer inputBuffer;
					InputMemoryBuffer inputMemoryBuffer;
					synchronized (this) {
						inputMemoryBuffer = inputBuffers.pollFirst();
						if (inputMemoryBuffer == null) {
							synchronized (readingConnectionsHaveRead) {
								readingConnectionsHaveRead.get(this.prioOrdinal).remove(this);
							}
							return null;
						}
					}
					inputBuffer = inputMemoryBuffer.getReadingBuffer();
					QpMessage m = null;
					int lastMsgSize = Integer.MIN_VALUE;
					while (m == null && inputBuffer.hasRemaining()) {
						ByteBuffer currInputBuffer;
						if (extraInputBuffer == null) {
							currInputBuffer = inputBuffer;
						} else {
							if (inputBuffer.remaining() <= extraInputBuffer.remaining()) {
								extraInputBuffer.put(inputBuffer);
							} else {
								byte[] data = new byte[extraInputBuffer.remaining()];
								inputBuffer.get(data);
								extraInputBuffer.put(data);
							}
							if (extraInputBuffer.remaining() == 0) {
								extraInputBuffer.flip();
								currInputBuffer = extraInputBuffer;
								extraInputBuffer = null;
							} else {
								// Input buffer has nothing left
								break;
							}
						}
						if (currMsgSize < 0) {
							if (currInputBuffer.remaining() >= 4) {
								currMsgSize = currInputBuffer.getInt();
								if (currMsgSize <= 0) {
									IllegalStateException e = new IllegalStateException("Message size should be positive");
									logger.error("Input buffer contents: " + SocketManager.toString(currInputBuffer) + ", position " + currInputBuffer.position(), e);
									throw e;
								}
							} else if (currInputBuffer.remaining() > 0) {
								extraInputBuffer = ByteBuffer.allocate(4);
								extraInputBuffer.put(currInputBuffer);
							}
						} else if (currMsgSize > currInputBuffer.remaining()) {
							extraInputBuffer = ByteBuffer.allocate(currMsgSize);
							extraInputBuffer.put(currInputBuffer);
						} else if (currInputBuffer.remaining() >= currMsgSize) {
							m = deserialize(currInputBuffer, currMsgSize);
							lastMsgSize = currMsgSize;
							currMsgSize = Integer.MIN_VALUE;
						} else {
							// Need to read more data for current message into
							// regular input buffer
						}
					}
					synchronized (this) {
						if (inputBuffer.remaining() > 0) {
							inputBuffers.addFirst(inputMemoryBuffer);
						} else {
							returnBuffer(inputBuffer);
							checkUnthrottle();
						}
						numBytesProcessed += lastMsgSize + 4; // include length of message length
						if (m != null) {
							if (logger.isTraceEnabled()) {
								logger.trace(bindAddress + " received message " + m.messageId + " from " + origin);
							}
							if (idBuf != null) {
								idBuf.writeLong(m.messageId);
								setWantsWrite(true);
							}
							return m;
						}
					}
				}
			}
		}

		private QpMessage deserialize(ByteBuffer buf, int msgSize) {
			txnInputBuf.init(buf, msgSize);
			QpMessage retval = null;
			try {
				retval = qms.deserialize(txnInputBuf, origin);
			} catch (Exception e) {
				logger.error("Error deserializing message, total length = " + msgSize + " bytes", e);
			}
			if (! txnInputBuf.hasEnded()) {
				logger.error("Deserialized message was not finished");
				try {
					txnInputBuf.skipTransaction();
				} catch (Exception e) {
					logger.error("Error skipping unfinished transaction", e);
				}
			}
			return retval;
		}

		@Override
		void remove() {
			synchronized (incoming) {
				incoming.remove(this);
			}
			synchronized (nodesToPing) {
				nodesToPing.remove(this.origin);
				nodesHaveBeenPinged.remove(this.origin);
			}
		}

		public String toString() {
			return "ReadingConnectionManager(" + origin + ")";
		}

		void hasWritten() {
		}

		@Override
		synchronized int byteBuffersInUse() {
			if (idBuf == null) {
				return 0;
			} else {
				return idBuf.numBuffersInUse();
			}
		}

		@Override
		synchronized long bytesCanBePulled() {
			if (idBuf == null) {
				return 0;
			}
			long numWritten = idBuf.numBytesWritten();
			if (numWritten == 4) {
				return 0;
			} else {
				return numWritten;
			}
		}

		@Override
		synchronized OutputMemoryBuffer pullUrgentBuffer() {
			if (throttled ^ remoteStateIsThrottled) {
				// throttled and remoteStateIsThrottled don't agree
				OutputMemoryBuffer retval = new OutputMemoryBuffer(4);
				retval.writeInt(throttled ? START_THROTTLING : STOP_THROTTLING);
				remoteStateIsThrottled = throttled;
				return retval;
			} else {
				return null;
			}
		}

		@Override
		synchronized OutputMemoryBuffer pullBuffer() {
			if (idBuf == null || idBuf.numBytesWritten() == 4) {
				return null;
			} else {
				OutputMemoryBuffer retval = idBuf;
				idBuf = new OutputMemoryBuffer(replyBufferSize);
				idBuf.writeInt(0);
				int numIds = (retval.numBytesWritten() - 4) / 8;
				retval.writeInt(numIds, 0);
				return retval;
			}
		}

		@Override
		synchronized void logStatus() {
			if (logger.isTraceEnabled()) {
				logger.trace("ReadingConnectionManager from " + origin + " to " + bindAddress + " has read " + this.bytesReceived + " bytes and processed " + this.numBytesProcessed + " bytes");
			}
		}

		private boolean throttled = false;
		private boolean remoteStateIsThrottled = false;

		private synchronized void throttle() {
			if (throttled) {
				return;
			}
			throttled = true;
			throttledBy.add(origin);
			if (! remoteStateIsThrottled) {
				logger.info(bindAddress + " requested that no data be sent from " + origin);
				setWantsWrite(true);
			}
		}

		private synchronized void unthrottle() {
			if (! throttled) {
				return;
			}
			throttled = false;
			throttledBy.remove(origin);
			if (remoteStateIsThrottled) {
				logger.info(bindAddress + " requested that data be sent from " + origin);
				setWantsWrite(true);
			}
		}

		@Override
		Priority getPriority() {
			if (this.prioOrdinal < 0) {
				return null;
			} else {
				return prios[this.prioOrdinal];
			}
		}
	}

	private OutputMemoryBuffer getAddressBuffer(QpMessage.Priority prio) {
		OutputMemoryBuffer wbuf = new OutputMemoryBuffer(256);
		// This assumes that the address and port fits into one buffer, which seems reasonable
		byte[] addrBytes = publicAddress.getAddress().getAddress();
		wbuf.writeBytesNoLength(addrBytes);
		wbuf.writeShort((short) publicAddress.getPort());
		wbuf.writeInt(prio.ordinal());
		return wbuf;
	}

	private class WritingConnectionManager extends ConnectionManager {

		final InetSocketAddress dest;
		volatile long lastPingReceived = -1;

		private final Deque<OutputMemoryBuffer> bufs;

		private long bytesEnqueued = 0;
		private int numMessagesEnqueued = 0;
		private boolean throttled = false;
		private final Priority prio;

		WritingConnectionManager(AddrAndPrio dest, SelectionKey sk) throws IOException {
			super(sk,dest.addr.equals(bindAddress) || dest.addr.equals(publicAddress), replyBufferSize, getAddressBuffer(dest.prio));
			bufs = new ArrayDeque<OutputMemoryBuffer>();
			this.dest = dest.addr;
			bytesEnqueued += getAddressBuffer(dest.prio).numBytesWritten();
			prio = dest.prio;
		}

		synchronized void sendWriteBuffer(OutputMemoryBuffer buf) {
			bytesEnqueued += buf.numBytesWritten();
			numMessagesEnqueued += buf.messageIds.size();
			if (bufs.isEmpty()) {
				bufs.addLast(buf);
			} else {
				OutputMemoryBuffer alreadyBuf = bufs.removeLast();
				if (alreadyBuf.readFrom(buf)) {
					bufs.addLast(alreadyBuf);
				} else if (buf.readFrom(alreadyBuf)) {
					bufs.addLast(buf);
				} else {
					bufs.addLast(alreadyBuf);
					bufs.addLast(buf);
				}
			}
			setWantsWrite(true);
		}

		BufferWriter getOutputBuffer() {
			return new BufferWriter();
		}

		class BufferWriter extends OutputMemoryBuffer implements TransactionalOutputBufferControls {
			int startPos = Integer.MIN_VALUE;

			BufferWriter() {
				super(msgBufferSize);
			}

			private boolean transactionInProgress() {
				return startPos >= 0;
			}

			public void beginTransaction() {
				if (transactionInProgress()) {
					throw new IllegalStateException("Transaction already in progress");
				}
				// Make sure we're not splitting 
				ensureAvailable(4);
				startPos = getWritingPos();
				writeInt(0);
			}

			public void commitTransaction(long msgId) {
				if (startPos < 0) {
					throw new IllegalStateException("No transaction is in progress");
				}
				// Don't include the length of the length
				int txnLength = getWritingPos() - startPos - 4; 
				writeInt(txnLength, startPos);
				addMessageId(msgId);
				startPos = Integer.MIN_VALUE;
			}

			public void rollbackTransaction() {
				if (startPos < 0) {
					throw new IllegalStateException("No transaction is in progress");
				}
				setWritingPos(startPos);
			}
		}

		private LongList receivedMsgs = new LongList();
		private int numIdsToRead = 0;

		@Override
		void hasRead() {
			LongList received = new LongList();
			InputMemoryBuffer tempInputMemoryBuffer = null;
			for ( ; ; ) {
				final InputMemoryBuffer inputMemoryBuffer;
				synchronized (this) {
					inputMemoryBuffer = inputBuffers.pollFirst();
					if (inputMemoryBuffer == null) {
						if (tempInputMemoryBuffer != null && tempInputMemoryBuffer.remainingToRead() > 0) {
							inputBuffers.addFirst(tempInputMemoryBuffer);
						}
						break;
					}
				}
				final ByteBuffer inputBuffer = inputMemoryBuffer.getReadingBuffer();
				if (tempInputMemoryBuffer != null) {
					ByteBuffer tempBuffer = tempInputMemoryBuffer.getWritingBuffer();
					while (tempBuffer.remaining() > 0 && inputBuffer.remaining() > 0) {
						tempBuffer.put(inputBuffer.get());
					}
					if (tempBuffer.remaining() > 0) {
						returnBuffer(inputBuffer);
						continue;
					}
				}
				while (inputBuffer.remaining() > 0) {
					if (numIdsToRead == 0) {
						if (tempInputMemoryBuffer == null) {
							if (inputBuffer.remaining() >= 4) {
								numIdsToRead = inputBuffer.getInt();
							} else {
								tempInputMemoryBuffer = new InputMemoryBuffer(ByteBuffer.allocate(4));
								ByteBuffer tempBuffer = tempInputMemoryBuffer.getWritingBuffer();
								tempBuffer.put(inputBuffer);
							}
						} else {
							ByteBuffer tempBuffer = tempInputMemoryBuffer.getReadingBuffer();
							numIdsToRead = tempBuffer.getInt();
							tempInputMemoryBuffer = null;
						}
					} else {
						if (tempInputMemoryBuffer != null) {
							ByteBuffer tempBuffer = tempInputMemoryBuffer.getReadingBuffer();
							received.add(tempBuffer.getLong());
							tempInputMemoryBuffer = null;
							--numIdsToRead;
						}
						while (numIdsToRead > 0 && inputBuffer.remaining() >= 8) {
							received.add(inputBuffer.getLong());
							--numIdsToRead;
						}
						if (numIdsToRead > 0 && inputBuffer.remaining() > 0) {
							tempInputMemoryBuffer = new InputMemoryBuffer(ByteBuffer.allocate(8));
							ByteBuffer tempBuffer = tempInputMemoryBuffer.getWritingBuffer();
							tempBuffer.put(inputBuffer);
						}
					}
					if (numIdsToRead < 0) {
						// Process a command
						if (numIdsToRead == START_THROTTLING) {
							throttle();
						} else if (numIdsToRead == STOP_THROTTLING) {
							unthrottle();
						} else {
							logger.error("Unknown command: "  + numIdsToRead);
						}
						numIdsToRead = 0;
					}
				}
				returnBuffer(inputBuffer);
			}
			if (received.size() != 0) {
				synchronized (this) {
					receivedMsgs.addFrom(received);
				}
			}


			synchronized (writingConnectionsHaveWrittenOrRead) {
				if (writingConnectionsHaveWrittenOrRead.add(this)) {
					writingConnectionsHaveWrittenOrRead.notifyAll();
				}
			}
		}

		private void sendMessagesReceived() {
			LongList toReport;
			synchronized (this) {
				if (receivedMsgs.size() > 0) {
					toReport = receivedMsgs;
					receivedMsgs = new LongList(toReport.size());
				} else {
					toReport = null;
				}
			}
			if (toReport != null) {
				client.sentMessagesReceived(dest, toReport);

			}
		}

		private void sendMessagesSent() {
			LongList sent = new LongList();
			synchronized (messagesSent) {
				if (messagesSent.size() == 0) {
					return;
				}
				sent.addFrom(messagesSent);
				messagesSent.clear();
			}
			client.messagesSent(sent);
		}

		@Override
		void remove() {
			synchronized (outgoing) {
				outgoing.remove(dest);
			}
		}

		public String toString() {
			return "WritingConnectionManager(" + dest + ")";
		}

		synchronized void close() throws IOException {
			super.close();
			client.peerIsDead(dest);
		}

		void hasWritten() {
			synchronized (writingConnectionsHaveWrittenOrRead) {
				if (writingConnectionsHaveWrittenOrRead.add(this)) {
					writingConnectionsHaveWrittenOrRead.notifyAll();
				}
			}
		}

		@Override
		synchronized int byteBuffersInUse() {
			int retval = 0;
			for (OutputMemoryBuffer buf : bufs) {
				retval += buf.numBuffersInUse();
			}
			return retval;
		}

		@Override
		synchronized long bytesCanBePulled() {
			long retval = 0;
			for (OutputMemoryBuffer buf : bufs) {
				retval += buf.numBytesWritten();
			}
			return retval;
		}

		@Override
		synchronized OutputMemoryBuffer pullBuffer() {
			return bufs.pollFirst();
		}

		@Override
		synchronized void logStatus() {
			if (logger.isTraceEnabled()) {
				logger.trace("WritingConnectionManager from " + bindAddress + " to " + dest + " has enqueued " + this.bytesEnqueued + " bytes for " + this.numMessagesEnqueued + " messages and sent " + this.bytesSent + " bytes");
			}
		}

		synchronized void setWantsWrite(boolean wakeup) {
			if (throttled) {
				return;
			}
			super.setWantsWrite(wakeup);
		}

		private synchronized void throttle() {
			if (throttled) {
				return;
			}
			synchronized (startedThrottling) {
				if (! stoppedThrottling.remove(dest)) {
					startedThrottling.add(dest);
				}
				startedThrottling.notify();
			}
			throttled = true;
			setDoesNotWantWrite();
			logger.info("SM: " + bindAddress + " stopped sending to " + dest);
		}

		private synchronized void unthrottle() {
			if (! throttled) {
				return;
			}
			synchronized (startedThrottling) {
				if (! startedThrottling.remove(dest)) {
					stoppedThrottling.add(dest);
				}
				startedThrottling.notify();
			}
			throttled = false;
			setWantsWrite(true);
			logger.info("SM: " + bindAddress + " started sending to " + dest);
		}

		@Override
		OutputMemoryBuffer pullUrgentBuffer() {
			return null;
		}

		@Override
		Priority getPriority() {
			return prio;
		}
	}

	private static String toString(ByteBuffer buf) {
		int length = buf.limit();
		byte[] data = new byte[length];
		for (int i = 0; i < length; ++i) {
			data[i] = buf.get(i);
		}
		return Arrays.toString(data);
	}

	/**
	 * Determine if sending has been suspended to this
	 * node from other nodes
	 * 
	 * @return		<code>true</code> if sending has been suspended,
	 * 				<code>false</code> if it has not
	 */
	boolean isThrottledByNodes() {
		synchronized (throttledBy) {
			return (! throttledBy.isEmpty());
		}
	}

	/**
	 * Determine if sending has been suspended to this node
	 * from another node
	 * 
	 * @param node	The node of interest
	 * @return		<code>true</code> if sending has been suspended,
	 * 				<code>false</code> if it has not
	 */
	boolean isThrottledBy(InetSocketAddress node) {
		synchronized (throttledBy) {
			return throttledBy.contains(node);
		}
	}

	/**
	 * Determine if sending has been suspended from this
	 * node to other nodes
	 * 
	 * @return		<code>true</code> if sending has been suspended,
	 * 				<code>false</code> if it has not
	 */
	boolean throttlesNodes() {
		synchronized (throttledNodes) {
			return (! throttledNodes.isEmpty());
		}
	}

	/**
	 * Determine if sending has been suspended from this node
	 * to another node
	 * 
	 * @param node	The node of interest
	 * @return		<code>true</code> if sending has been suspended,
	 * 				<code>false</code> if it has not
	 */
	boolean throttles(InetSocketAddress node) {
		synchronized (throttledNodes) {
			return throttledNodes.contains(node);
		}
	}

	/**
	 * Get the collection of nodes that have temporarily stopped
	 * sending to this node
	 * 
	 * @return		The collection of nodes
	 */
	Collection<InetSocketAddress> getThrottledByNodes() {
		synchronized (throttledBy) {
			if (throttledBy.isEmpty()) {
				return Collections.emptyList();
			}
			return new ArrayList<InetSocketAddress>(throttledBy);
		}
	}

	/**
	 * Get the collection of nodes that this node has temporarily
	 * stopped sending to
	 * 
	 * @return	The collection of nodes
	 */
	Collection<InetSocketAddress> getThrottledNodes() {
		synchronized (throttledNodes) {
			if (throttledNodes.isEmpty()) {
				return Collections.emptyList();
			}
			return new ArrayList<InetSocketAddress>(throttledNodes);
		}
	}

	private volatile boolean forcedThrottling = false;


	void startForcedThrottling() {
		forcedThrottling = true;
	}

	void stopForcedThrottling() {
		forcedThrottling = false;
		List<ReadingConnectionManager> rcms;
		synchronized (incoming) {
			rcms = new ArrayList<ReadingConnectionManager>(incoming);
		}
		for (ReadingConnectionManager rcm : rcms) {
			rcm.checkUnthrottle();
		}
	}
}
