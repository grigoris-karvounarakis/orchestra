package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class QueryTornDown extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final long totalBytesSent;
	
	public QueryTornDown(TearDownQueryMessage inReplyTo, long totalBytesSent) {
		super(inReplyTo);
		this.totalBytesSent = totalBytesSent;
	}

	public QueryTornDown(InputBuffer buf, InetSocketAddress origin)
			throws SerializationException {
		super(buf, origin);
		totalBytesSent = buf.readLong();
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeLong(totalBytesSent);
	}

}
