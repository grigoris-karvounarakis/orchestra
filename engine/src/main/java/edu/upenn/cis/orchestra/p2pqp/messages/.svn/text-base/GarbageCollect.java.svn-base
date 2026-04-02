package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class GarbageCollect extends QpMessage {
	private static final long serialVersionUID = 1L;

	public GarbageCollect(InetSocketAddress dest) {
		super(dest);
	}

	public GarbageCollect(InputBuffer buf, InetSocketAddress origin)
			throws SerializationException {
		super(buf, origin);
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
	}

}
