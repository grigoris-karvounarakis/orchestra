package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;


public class ReplySuccess extends QpMessage {
	private static final long serialVersionUID = 1L;

	public ReplySuccess(InetSocketAddress dest, long[] ids) {
		super(dest,ids);
	}

	public ReplySuccess(QpMessage origMessage) {
		super(origMessage, false);
	}

	public String toString() {
		return "ReplySuccess";
	}

	public ReplySuccess(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
	}

	protected void subclassSerialize(OutputBuffer buf) {
	}
}
