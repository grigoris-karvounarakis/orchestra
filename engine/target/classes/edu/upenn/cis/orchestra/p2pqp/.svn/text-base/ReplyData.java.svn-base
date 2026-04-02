package edu.upenn.cis.orchestra.p2pqp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.TimerTask;

import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.p2pqp.messages.ReplyTimeout;

class ReplyData {
	final ReplyContinuation rc;
	final long msgId;
	private final int retryDelay;
	private final int replyTimeout;
	private int retriesRemaining;
	private TimerTask pendingAction;
	final Class<?> successReplies[];
	private boolean hasFinished;
	private boolean receivedTimeoutOrFailure;
	private boolean sent;
	private boolean retryImmediately;
	final boolean retryable;
	
	InetSocketAddress sentTo;

	ReplyData(ReplyContinuation rc, long msgId, boolean retryable, int retryDelay, int numRetries, int replyTimeout, Class<?> successReplies[]) {
		this.rc = rc;
		this.msgId = msgId;
		this.retryDelay = retryDelay;
		this.retriesRemaining = numRetries;
		this.replyTimeout = replyTimeout;
		this.successReplies = successReplies;
		this.retryable = retryable; 
		hasFinished = false;
		sent = false;
		receivedTimeoutOrFailure = false;
	}
			
	synchronized void cancelPendingAction() {
		if (pendingAction != null) {
			pendingAction.cancel();
			pendingAction = null;
		}
	}
	
	synchronized void startPendingAction(QpApplication<?> app) throws IOException, InterruptedException, SerializationException {
		if (hasFinished) {
			return;
		}
		
		cancelPendingAction();

		if (app.socketManager.throttles(sentTo)) {
			return;
		}
		
		if (receivedTimeoutOrFailure) {
			sent = false;
			if (retryImmediately) {
				resend(app);
			} else if (retryable) {
				pendingAction = app.scheduleResendMessage(this, retryDelay);
			}
		} else if (sent && replyTimeout != 0) {
			pendingAction = app.scheduleDeliverMessage(new ReplyTimeout(msgId), replyTimeout);
		}
	}

	synchronized void setSentTo(InetSocketAddress dest) {
		sentTo = dest;
	}
			
	synchronized void recordFailure(boolean retryImmediately) {
		this.receivedTimeoutOrFailure = true;
		--retriesRemaining;
		this.retryImmediately = retryImmediately;
	}
	
	synchronized void resend(QpApplication<?> app) throws IOException, InterruptedException, SerializationException {
		if (! this.receivedTimeoutOrFailure) {
			return;
		}
		this.receivedTimeoutOrFailure = false;
		app.sendMessageAwaitReply(this);
	}
	
	synchronized boolean hasFinished() {
		return hasFinished;
	}
	
	synchronized void setFinished() {
		hasFinished = true;
		cancelPendingAction();
	}
	
	synchronized boolean retriesLeft() {
		return retriesRemaining > 0;
	}
	
	synchronized void setSent() {
		this.sent = true;
		this.receivedTimeoutOrFailure = false;
	}	
}