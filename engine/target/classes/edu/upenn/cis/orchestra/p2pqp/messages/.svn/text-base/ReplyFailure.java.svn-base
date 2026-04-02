package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ReplyFailure extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final String why;

	public ReplyFailure(QpMessage origMessage, String why, boolean canRetry) {
		super(origMessage, canRetry);
		this.why = why;
	}
	
	public String toString() {
		return "ReplyFailure(" + why + ")";
	}

	public ReplyFailure(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		this.why = buf.readString();
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeString(why);
	}
}
