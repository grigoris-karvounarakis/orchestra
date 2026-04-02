package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class DistributeQueryContinueMessage extends QpMessage {
	private static final long serialVersionUID = 1L;

	public final int epoch;
	public final int queryId;

	public DistributeQueryContinueMessage(InetSocketAddress dest, int epoch, int queryId) {
		super(dest);
		this.epoch = epoch;
		this.queryId = queryId;
	}

	public String toString() {
		return "DistributeQueryContinueMessage(" + queryId + ")";
	}
	
	public DistributeQueryContinueMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		epoch = buf.readInt();
		queryId = buf.readInt();
	}
	
	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(epoch);
		buf.writeInt(queryId);
	}
}
