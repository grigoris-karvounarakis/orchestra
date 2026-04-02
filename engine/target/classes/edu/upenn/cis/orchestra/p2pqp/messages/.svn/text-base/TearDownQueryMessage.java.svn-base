package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class TearDownQueryMessage extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int queryId;

	public TearDownQueryMessage(InetSocketAddress dest, int queryId) {
		super(dest);
		this.queryId = queryId;
	}

	public String toString() {
		return "TearDownQueryMessage(" + queryId + ")";
	}

	public TearDownQueryMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		queryId = buf.readInt();
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(queryId);
	}
}