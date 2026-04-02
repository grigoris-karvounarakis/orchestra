package edu.upenn.cis.orchestra.p2pqp;

abstract class ReplyContinuation {
	/**
	 * Method to call when a reply comes in. If more retries are still possible,
	 * failures (as defined when the message was sent) will not be sent here, and the
	 * message will be resent automatically. May also receive a
	 * {@link ReplyTimeout} if the last retry times out.
	 * 
	 * @param m The message received in reply
	 */
	abstract void processReply(QpMessage m);
	
	
	/**
	 * @return	True if the continuation is not needed anymore, false
	 * if additional replies are expected.
	 */
	abstract boolean isFinished();
	
	void sent() {
	}
	
	void received() {
	}
}