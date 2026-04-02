package edu.upenn.cis.orchestra.reconciliation.p2pstore;


import edu.upenn.cis.orchestra.reconciliation.p2pstore.P2PStore.MessageProcessorThread;


class SimpleReplyContinuation<T> implements ReplyContinuation {
	private final ReplyHolder<? super T> replies;
	private final T key;
	private boolean finished;
	
	SimpleReplyContinuation(T key, ReplyHolder<? super T> replies) {
		this.key = key;
		this.replies = replies;
		finished = false;
	}
	
	
	public boolean isFinished() {
		return finished;
	}

	public void processReply(P2PMessage m, MessageProcessorThread mpt) {
		replies.receiveReply(key, m);
		finished = true;
	}

}
