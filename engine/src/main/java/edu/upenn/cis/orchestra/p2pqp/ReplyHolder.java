package edu.upenn.cis.orchestra.p2pqp;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

class ReplyHolder<T> implements Iterable<QpMessage> {
	private int numRepliesRemaining;
	private Map<T,QpMessage> replies;

	ReplyHolder(int numReplies) {
		numRepliesRemaining = numReplies;
		replies = new HashMap<T,QpMessage>();
	}
	
	synchronized void addMoreRepliesExpected(int numAdditionalReplies) {
		numRepliesRemaining += numAdditionalReplies;
	}
	
	synchronized boolean receiveReply(T key, QpMessage reply) {
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
	
	synchronized void waitUntilFinished() throws InterruptedException {
		while (numRepliesRemaining > 0) {
			wait();
		}
	}
	
	synchronized int numRepliesRemaining() {
		return numRepliesRemaining;
	}
	
	synchronized QpMessage getReply(T key) {
		return replies.get(key);
	}
	
	void reset(int numReplies) {
		replies.clear();
		numRepliesRemaining = numReplies;
	}
	
	public Iterator<QpMessage> iterator() {
		return Collections.unmodifiableCollection(replies.values()).iterator();
	}
	
	public Set<T> getKeys() {
		return Collections.unmodifiableSet(replies.keySet());
	}
}
