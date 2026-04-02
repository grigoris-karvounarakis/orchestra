package edu.upenn.cis.orchestra.p2pqp.messages;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ReplySendingFailed extends QpMessage {
	private static final long serialVersionUID = 1L;

	public final long origMsgId;
	public final boolean canRetry;
	
	public ReplySendingFailed(long origMsgId, boolean canRetry) {
		super(null, new long[] {origMsgId});
		this.origMsgId = origMsgId;
		this.canRetry = canRetry;
	}
	
	public long[] getOrigIds() {
		return new long[] {origMsgId};
	}
	
	protected boolean canRetry() {
		return canRetry;
	}

	protected boolean retryImmediately() {
		return true;
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		throw new UnsupportedOperationException();
	}
}
