package edu.upenn.cis.orchestra.reconciliation.p2pstore;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;

class ReplyHolder<T> implements Iterable<P2PMessage> {
	private int numRepliesRemaining;
	private Map<T,P2PMessage> replies;

	ReplyHolder(int numReplies) {
		numRepliesRemaining = numReplies;
		replies = new HashMap<T,P2PMessage>();
	}
	
	synchronized boolean receiveReply(T key, P2PMessage reply) {
		if (replies.containsKey(key)) {
			return false;
		}
		
		replies.put(key, reply);
		--numRepliesRemaining;
		
		if (numRepliesRemaining <= 0) {
			notifyAll();
		}
		
		return true;
	}
	
	void waitUntilFinished() throws InterruptedException {
		waitUntilFinished(null);
	}
	
	synchronized void waitUntilFinished(P2PStore.MessageProcessorThread processor) throws InterruptedException {
		if (processor != null) {
			processor.startAwaitingReply();
		}
		while (numRepliesRemaining > 0) {
			wait();
		}
		if (processor != null) {
			processor.stopAwaitingReply();
		}
	}
	
	synchronized int numRepliesRemaining() {
		return numRepliesRemaining;
	}
	
	synchronized P2PMessage getReply(T key) {
		return replies.get(key);
	}
	
	void reset(int numReplies) {
		replies.clear();
		numRepliesRemaining = numReplies;
	}

	public Iterator<P2PMessage> iterator() {
		return Collections.unmodifiableCollection(replies.values()).iterator();
	}
	
	public Set<T> getKeys() {
		return Collections.unmodifiableSet(replies.keySet());
	}
}
