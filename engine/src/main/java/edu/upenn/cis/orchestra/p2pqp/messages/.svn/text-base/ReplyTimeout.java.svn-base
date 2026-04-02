package edu.upenn.cis.orchestra.p2pqp.messages;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ReplyTimeout extends QpMessage {
	private static final long serialVersionUID = 1L;

	public ReplyTimeout(long msgId) {
		super(null, new long[] {msgId});
	}

	public String toString() {
		return "ReplyTimeout";
	}
		
	protected boolean canRetry() {
		return true;
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		throw new UnsupportedOperationException();
	}
}


