package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

class MsgStatusCache {
	// head is least recently used, tail is most recently used
	private ListEl head, tail;
	private final ConcurrentMap<MsgId,ListEl> findEl;
	private final ConcurrentLinkedQueue<ListEl> touched;
	private final ConcurrentLinkedQueue<ListEl> removed;
	private volatile long currentTime;

	private final int tidyIntervalMsec;
	private final long holdTimeNsec;

	private Thread checkThread;

	MsgStatusCache(int holdTimeMsec) {
		head = null;
		tail = null;
		findEl = new ConcurrentHashMap<MsgId,ListEl>(10000, 0.75f, QpApplication.numMessageProcessingThreads + 1);
		touched = new ConcurrentLinkedQueue<ListEl>();
		removed = new ConcurrentLinkedQueue<ListEl>();
		currentTime = System.nanoTime();
		holdTimeNsec = holdTimeMsec * 1000000L;
		this.tidyIntervalMsec = holdTimeMsec * 2;
		reopen();
	}

	void close() throws InterruptedException {
		checkThread.interrupt();
		checkThread.join();
	}

	void reopen() {
		checkThread = new TidyThread();
		checkThread.start();
	}

	synchronized void clear() {
		touched.clear();
		findEl.clear();
		head = null;
		tail = null;
	}

	private static class ListEl {
		ListEl next;
		ListEl prev;
		final MsgId key;
		private long lastTime;
		private MsgStatus status;
		boolean deleted = false;

		ListEl(MsgId key, long lastTime) {
			this.key = key;
			this.lastTime = lastTime;
			this.status = MsgStatus.RECEIVED;
		}

		synchronized void setTime(long time) {
			lastTime = time;
		}

		synchronized long getTime() {
			return lastTime;
		}

		synchronized void setStatus(MsgStatus status) {
			this.status = status;
		}

		synchronized MsgStatus getStatus() {
			return status;
		}
	}



	static class MsgId {
		final InetSocketAddress from;
		final long id;

		MsgId(InetSocketAddress from, long id) {
			this.from = from;
			this.id = id;
		}

		public boolean equals(Object o) {
			if (o == null || o.getClass() != MsgId.class) {
				return false;
			}
			MsgId mi = (MsgId) o;
			return (id == mi.id && from.equals(mi.from));
		}

		public int hashCode() {
			return ((int) id) + from.hashCode();
		}

		public String toString() {
			return "(" + from + "," + id + ")";
		}
	}

	enum MsgStatus {
		RECEIVED,
		SUCCEEDED,
		FAILED,
		CAN_RETRY,
		CANNOT_RETRY,
		UNKNOWN
	};

	void recordMessageReceived(InetSocketAddress from, long msgId) {
		recordMsgReceivedPrivate(new MsgId(from,msgId));
	}
	
	ListEl recordMsgReceivedPrivate(MsgId id) {
		ListEl el = new ListEl(id, currentTime);
		synchronized (this) {
			findEl.put(id, el);
		}
		updateQueue(el);
		return el;
	}
	
	@SuppressWarnings("unused")
	private void remove(InetSocketAddress from, long msgId) {
		ListEl el = findEl.remove(new MsgId(from,msgId));
		if (el == null) {
			return;
		}
		removed.add(el);
	}

	void recordMessageSuccess(InetSocketAddress from, long msgId) {
		updateStatus(new MsgId(from,msgId), MsgStatus.SUCCEEDED);
	}

	void recordMessageFailure(InetSocketAddress from, long msgId, boolean canRetry) {
		updateStatus(new MsgId(from,msgId), canRetry ? MsgStatus.CAN_RETRY : MsgStatus.FAILED);
	}
	
	void recordMessageFinished(InetSocketAddress from, long msgId, boolean canRetry) {
		updateStatus(new MsgId(from,msgId), canRetry ? MsgStatus.CAN_RETRY : MsgStatus.CANNOT_RETRY);
	}

	private void updateStatus(MsgId id, MsgStatus status) {
		ListEl el = findEl.get(id);
		if (el == null) {
			recordMsgReceivedPrivate(id).setStatus(status);
		} else {
			el.setStatus(status);
			updateQueue(el);
		}
	}

	MsgStatus getStatus(InetSocketAddress from, long msgId) {
		ListEl el = findEl.get(new MsgId(from, msgId));
		if (el == null) {
			return MsgStatus.UNKNOWN;
		} else {
			return el.getStatus();
		}
	}

	private void updateQueue(ListEl el) {
		el.setTime(currentTime);
		touched.add(el);
	}

	private class TidyThread extends Thread {
		TidyThread() {
			super("MsgStatusCache TidyThread");
		}
		public void run() {
			while (! this.isInterrupted()) {
				try {
					Thread.sleep(tidyIntervalMsec);
				} catch (InterruptedException e) {
					return;
				}
				// Put the queue in order
				ListEl el;
				
				while ((el = removed.poll()) != null) {
					if (el.deleted) {
						continue;
					}
					el.deleted = true;
					if (el == head) {
						head = el.next;
						if (head == null) {
							tail = null;
						} else {
							head.prev = null;
						}
					} else if (el == tail) {
						tail = el.prev;
						tail.next = null;
					} else {
						if (el.prev != null) {
							el.prev.next = el.next;
						}
						if (el.next != null) {
							el.next.prev = el.prev;
						}
					}
				}
				
				
				while ((el = touched.poll()) != null) {
					if (el.deleted) {
						continue;
					}
					if (head == null) {
						head = el;
						tail = el;
						el.next = null;
						el.prev = null;
						continue;
					}
					if (el == tail) {
						continue;
					} else if (el == head) {
						head = el.next;
						head.prev = null;
					} else {
						if (el.prev != null) {
							el.prev.next = el.next;
						}
						if (el.next != null) {
							el.next.prev = el.prev;
						}
					}

					tail.next = el;
					el.prev = tail;
					tail = el;
					tail.next = null;
				}

				long time = currentTime;

				List<MsgId> toDispose = new ArrayList<MsgId>();

				while (head != null) {
					if (time - head.getTime() < holdTimeNsec) {
						// Following elements are too recent to throw away
						break;
					}
					head.deleted = true;
					head = head.next;
					if (head == null) {
						tail = null;
					} else {
						head.prev = null;
						toDispose.add(head.key);
					}
				}

				time = System.nanoTime();

				for (MsgId id : toDispose) {
					findEl.remove(id);
				}
			}
		}
	}
}
