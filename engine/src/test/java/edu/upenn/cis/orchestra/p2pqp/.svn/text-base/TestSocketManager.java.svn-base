package edu.upenn.cis.orchestra.p2pqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.upenn.cis.orchestra.p2pqp.Calibrate.TestClient;
import edu.upenn.cis.orchestra.p2pqp.messages.DummyMessage;


public class TestSocketManager {
	int port1 = 5000, port2 = 6000;
	InetSocketAddress address1, address2;

	SocketManager sm1, sm2;
	TestClient tc1, tc2;

	@BeforeClass
	public static void initLogging() {
		Logger.getRootLogger().setLevel(Level.INFO);
		Logger.getRootLogger().removeAllAppenders();
		Logger.getRootLogger().addAppender(new ConsoleAppender(new SimpleLayout()));
	}

	private void setUp(int msgBufferSize, int replyBufferSize, int maxMemoryBuffers) throws Exception {
		try {
			InetAddress localHost = InetAddress.getLocalHost();

			QpMessageSerialization qms = new QpMessageSerialization();

			File scratchDir = getScratchDir();
			ScratchFileGenerator sfg = new SimpleScratchFileGenerator(scratchDir, "temp");

			tc1 = new TestClient();
			tc2 = new TestClient();

			ThreadGroup tg = new ThreadGroup("TestSocketManager");

			address1 = new InetSocketAddress(localHost, port1);
			address2 = new InetSocketAddress(localHost, port2);

			sm1 = new SocketManager(qms, sfg, tc1, tg, address1, address1, msgBufferSize, replyBufferSize, maxMemoryBuffers);
			sm2 = new SocketManager(qms, sfg, tc2, tg, address2, address2, msgBufferSize, replyBufferSize, maxMemoryBuffers);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
	}

	private static File getScratchDir() {
		File scratchDir = new File("scratch");
		if (scratchDir.exists()) {
			for (File f : scratchDir.listFiles()) {
				f.delete();
			}
		} else {
			scratchDir.mkdir();
		}
		return scratchDir;
	}

	@Test
	public void testSendOne() throws Exception {
		System.out.println("testSendOne");
		setUp(8092, 4096, 12);
		testSend(1, false);
	}

	@Test
	public void testSendFew() throws Exception {
		System.out.println("testSendFew");
		setUp(8092, 4096, 12);
		testSend(20, false);
	}

	@Test
	public void testSendMany() throws Exception {
		System.out.println("testSendMany");
		setUp(512, 30, 64);
		testSend(3000, false);
	}

	@Test
	public void testSendManyMixedPrios() throws Exception {
		System.out.println("testSendManyMixedPrios");
		setUp(512, 30, 64);
		testSend(3000, true);
	}
	
	private void testSend(int count, boolean mixPrios) throws Exception {
		tc2.setExpectedMessages(count);
		for (int i = 0; i < count; ++i) {
			QpMessage m;
			if (mixPrios) {
				m = new DummyMessage(address2, i % 2 == 0 ? QpMessage.Priority.HIGH : QpMessage.Priority.NORMAL);
			} else {
				m = new DummyMessage(address2);
			}
			m.send(null,null, sm1);
		}

		System.out.println("Enqueued " + count + " messages");

		tc2.receiveMessages(sm2);
		System.out.println("Received " + count + " messages");

		synchronized (tc1) {
			while (tc1.received.size() < count) {
				tc1.wait();
			}
		}

		synchronized (tc1) {
			while (tc1.sent.size() < count) {
				tc1.wait();
			}
		}


		assertEquals("Incorrect number of messages reported as received", count, tc1.received.size());
		assertEquals("Incorrect number of messages reported as sent", count, tc1.sent.size());
	}

	@Test
	public void testSendError() throws Exception {
		System.out.println("testSendError");
		setUp(8092, 4096, 12);
		InetSocketAddress bad = new InetSocketAddress(InetAddress.getByName("www.seas.upenn.edu"),90);
		InetSocketAddress bad2 = new InetSocketAddress(InetAddress.getByName("dbclust1.cis.upenn.edu"),90);
		DummyMessage dm = new DummyMessage(bad,false);
		sm1.sendMessage(dm);
		DummyMessage dm2 = new DummyMessage(bad2,false);
		sm1.sendMessage(dm2);
		assertTrue("Should have bad peer notification", tc1.deadPeers.contains(bad));
		assertTrue("Should have bad peer notification", tc1.deadPeers.contains(bad2));
		Set<Long> failedMsgs = new HashSet<Long>();
		Set<Long> expectedFailed = new HashSet<Long>();
		expectedFailed.add(dm.messageId);
		expectedFailed.add(dm2.messageId);
		for (long msgId : tc1.failed.getList()) {
			failedMsgs.add(msgId);
		}
		assertEquals("Should have message sending failed notification",expectedFailed,failedMsgs);
	}

	@After
	public void tearDown() throws Exception {
		if (sm1 != null) {
			sm1.close();
		}
		if (sm2 != null) {
			sm2.close();
		}
	}

	@Test
	public void testTimeout() throws Exception {
		System.out.println("testTimeout");

		final int numNodes = 20;

		List<SocketManager> sms = new ArrayList<SocketManager>(numNodes);
		List<TestClient> tcs = new ArrayList<TestClient>(numNodes);
		List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();

		QpMessageSerialization qms = new QpMessageSerialization();
		InetAddress localHost = InetAddress.getLocalHost();

		ThreadGroup tg = new ThreadGroup("TestSocketManager");
		File scratchDir = getScratchDir();
		ScratchFileGenerator sfg = new SimpleScratchFileGenerator(scratchDir, "temp");
		for (int i = 0; i < numNodes; ++i) {
			InetSocketAddress address = new InetSocketAddress(localHost, port1 + i);
			TestClient tc = new TestClient();
			SocketManager sm = new SocketManager(qms, sfg, tc, tg, address, address, 8092, 4096, 64);
			sms.add(sm);
			tcs.add(tc);
			addresses.add(address);
		}

		// Open all pairwise connections
		for (int i = 0; i < numNodes; ++i) {
			for (int j = 0; j < numNodes; ++j) {
				QpMessage m = new DummyMessage(addresses.get(j));
				m.send(null,null, sms.get(i));
			}
		}

		Thread.sleep(20000);
		try {
			for (int i = 0; i < numNodes; ++i) {
				assertEquals("Incorrect set of failed nodes at " + addresses.get(i), Collections.EMPTY_SET, tcs.get(i).deadPeers);
			}
		} finally {
			for (SocketManager sm : sms) {
				sm.close();
			}
		}
	}

	@Test
	public void testMultithreadedSend() throws Exception {
		System.out.println("testMultithreadedSend");
		setUp(8092, 4096, 64);

		final int numThreads = 5;
		final int msgsPerThread = 20000;
		final int numMsgs = numThreads * msgsPerThread;

		tc2.setExpectedMessages(numMsgs);

		Thread statusThread = new Thread() {
			public void run() {
				try {
					synchronized (tc1) {
						while (tc1.sent.size() < numMsgs) {
							tc1.wait();
						}
						System.out.println("Sent " + numMsgs + " messages");
						while (tc1.received.size() < numMsgs) {
							tc1.wait();
						}
						System.out.println("Received acknowledgements of " + numMsgs + " messages");
					}
				} catch (InterruptedException ie) {
					return;
				}
			}
		};
		statusThread.start();

		final List<Thread> sendThreads = new ArrayList<Thread>(numThreads);
		for (int i = 0; i < numThreads; ++i) {
			Thread t = new Thread() {
				public void run() {
					try {
						for (int i = 0; i < msgsPerThread; ++i) {
							QpMessage m = new DummyMessage(address2);
							m.send(null,null, sm1);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};

			t.start();
			sendThreads.add(t);
		}

		Thread joinThread = new Thread() {
			public void run() {
				try {
					for (Thread t : sendThreads) {
						t.join();
					}
					System.out.println("Enqueued " + numMsgs + " messages");
				} catch (InterruptedException ie) {
					return;
				}
			}
		};
		joinThread.start();

		tc2.receiveMessages(sm2);
		System.out.println("Received " + numMsgs + " messages");

		joinThread.join();
		statusThread.join();
	}

	@Test
	public void testMultithreadedReceive() throws Exception {
		System.out.println("testMultithreadedReceive");
		setUp(8092, 4096, 64);

		final int numThreads = 5;
		final int numMsgs = 100000;

		tc2.setExpectedMessages(numMsgs);

		final List<Thread> receiveThreads = new ArrayList<Thread>(numThreads);
		for (int i = 0; i < numThreads; ++i) {
			Thread t = new Thread() {
				public void run() {
					try {
						tc2.receiveMessages(sm2);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};

			t.start();
			receiveThreads.add(t);
		}

		Thread statusThread = new Thread() {
			public void run() {
				try {
					synchronized (tc1) {
						while (tc1.sent.size() < numMsgs) {
							tc1.wait();
						}
						System.out.println("Sent " + numMsgs + " messages");
						while (tc1.received.size() < numMsgs) {
							tc1.wait();
						}
						System.out.println("Received acknowledgements of " + numMsgs + " messages");
					}
				} catch (InterruptedException ie) {
					return;
				}
			}
		};
		statusThread.start();


		Thread joinThread = new Thread() {
			public void run() {
				try {
					for (Thread t : receiveThreads) {
						t.join();
					}
					System.out.println("Received " + numMsgs + " messages");
				} catch (InterruptedException ie) {
					return;
				}
			}
		};
		joinThread.start();

		for (int i = 0; i < numMsgs; ++i) {
			QpMessage m = new DummyMessage(address2);
			m.send(null,null, sm1);
		}
		System.out.println("Enqueued " + numMsgs + " messages");


		joinThread.join();
		statusThread.join();
	}

	@Test
	public void testMultithreadSendAndReceive() throws Exception {
		System.out.println("testMultithreadedSendAndReceive");
		setUp(8092, 4096, 64);

		final int numThreads = 5;
		final int msgsPerThread = 20000;
		final int numMsgs = numThreads * msgsPerThread;

		tc2.setExpectedMessages(numMsgs);

		Thread statusThread = new Thread() {
			public void run() {
				try {
					synchronized (tc1) {
						while (tc1.sent.size() < numMsgs) {
							tc1.wait();
						}
						System.out.println("Sent " + numMsgs + " messages");
						while (tc1.received.size() < numMsgs) {
							tc1.wait();
						}
						System.out.println("Received acknowledgements of " + numMsgs + " messages");
					}
				} catch (InterruptedException ie) {
					return;
				}
			}
		};
		statusThread.start();

		final List<Thread> receiveThreads = new ArrayList<Thread>(numThreads);
		for (int i = 0; i < numThreads; ++i) {
			Thread t = new Thread() {
				public void run() {
					try {
						tc2.receiveMessages(sm2);
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};

			t.start();
			receiveThreads.add(t);
		}


		final List<Thread> sendThreads = new ArrayList<Thread>(numThreads);
		for (int i = 0; i < numThreads; ++i) {
			Thread t = new Thread() {
				public void run() {
					try {
						for (int i = 0; i < msgsPerThread; ++i) {
							QpMessage m = new DummyMessage(address2);
							m.send(null,null, sm1);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};

			t.start();
			sendThreads.add(t);
		}

		Thread joinThread = new Thread() {
			public void run() {
				try {
					for (Thread t : sendThreads) {
						t.join();
					}
					System.out.println("Enqueued " + numMsgs + " messages");

					for (Thread t : receiveThreads) {
						t.join();
					}
					System.out.println("Received " + numMsgs + " messages");
				} catch (InterruptedException ie) {
					return;
				}
			}
		};
		joinThread.start();

		joinThread.join();
		statusThread.join();
	}
	
	@Test
	public void testForcedThrottling() throws Exception {
		System.out.println("testForcedThrottling");
		setUp(8092, 4096, 12);

		int count = 1000;

		tc2.setExpectedMessages(count);
		
		new Thread() {
			public void run() {
				try {
					tc2.receiveMessages(sm2);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}.start();

		
		sm2.startForcedThrottling();
		QpMessage m = new DummyMessage(address2);
		m.send(null,null, sm1);
		
		Thread.sleep(1000);
		
		for (int i = 0; i < count; ++i) {
			m = new DummyMessage(address2);
			m.send(null,null, sm1);
		}
		
		assertTrue("Incorrect number of remaining messages", 999 <= tc2.remaining());

		System.out.println("Sent " + count + " messages");

		sm2.stopForcedThrottling();
		
		synchronized (tc2) {
			while (tc2.remaining() > 0) {
				tc2.wait();
			}
		}
		
	}
}
