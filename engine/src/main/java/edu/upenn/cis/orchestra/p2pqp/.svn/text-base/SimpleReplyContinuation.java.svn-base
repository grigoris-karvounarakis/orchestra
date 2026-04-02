package edu.upenn.cis.orchestra.p2pqp;

class SimpleReplyContinuation<T> extends ReplyContinuation {
	private final ReplyHolder<? super T> replies;
	private final T key;
	private boolean finished;
	
	SimpleReplyContinuation(T key, ReplyHolder<? super T> replies) {
		this.key = key;
		this.replies = replies;
		finished = false;
	}
	
	
	public synchronized boolean isFinished() {
		return finished;
	}

	public synchronized void processReply(QpMessage m) {
		replies.receiveReply(key, m);
		finished = true;
	}

}
