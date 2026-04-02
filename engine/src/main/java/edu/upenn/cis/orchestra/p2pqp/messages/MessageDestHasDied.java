package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.Collection;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class MessageDestHasDied extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final InetSocketAddress origMsgDest;
	private final long[] origMsgIds;
	private final boolean canRetry;

	private static long[] createArray(Collection<Long> msgIds) {
		long[] retval = new long[msgIds.size()];
		int pos = 0;
		for (long l : msgIds) {
			retval[pos++] = l;
		}
		return retval;
	}
	
	public MessageDestHasDied(InetSocketAddress origMsgDest, Collection<Long> msgIds, boolean canRetry) {
		super(null, createArray(msgIds));
		this.origMsgDest = origMsgDest;
		this.canRetry = canRetry;
		origMsgIds = new long[msgIds.size()];
		int pos = 0;
		for (long id : msgIds) {
			origMsgIds[pos++] = id;
		}
	}

	
	public String toString() {
		return "MessageDestHasDied(" + origMsgDest + ")";
	}
	
	public boolean canRetry() {
		return canRetry;
	}
	
	public long[] getOrigIds() {
		return origMsgIds;
	}
		
	protected boolean retryImmediately() {
		return true;
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		throw new UnsupportedOperationException();
	}
}
