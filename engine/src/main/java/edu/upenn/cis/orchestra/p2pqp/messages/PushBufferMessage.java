package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Operator;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class PushBufferMessage extends QpMessage implements
		QueryExecutionMessage {

	private final boolean distributedDest;
	private final String namedDest;
	private final int queryId;
	public final int operatorId;
	public final Operator.WhichInput whichInput;
	
	public PushBufferMessage(boolean distributedDest, String namedDest, int queryId, int operatorId, Operator.WhichInput whichInput) {
		super((InetSocketAddress) null);
		this.distributedDest = distributedDest;
		this.namedDest = namedDest;
		this.queryId = queryId;
		this.operatorId = operatorId;
		this.whichInput = whichInput;
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean centralDest() {
		return (namedDest == null && (! distributedDest));
	}

	@Override
	public boolean distributedDest() {
		return distributedDest;
	}

	@Override
	public int getQueryId() {
		return queryId;
	}

	@Override
	public String namedDest() {
		return namedDest;
	}

}
