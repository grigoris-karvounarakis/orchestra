package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class DoesNotHaveQuery extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int queryId;
	public DoesNotHaveQuery(QpMessage inReplyTo, int queryId) {
		super(inReplyTo, false);
		this.queryId = queryId;
	}
	public DoesNotHaveQuery(InputBuffer buf, InetSocketAddress origin)
			throws SerializationException {
		super(buf, origin);
		queryId = buf.readInt();
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(queryId);
	}

	public String toString() {
		return "DoesNotHaveQuery(" + queryId + " at " + this.getOrigin() + ")";
	}
}
