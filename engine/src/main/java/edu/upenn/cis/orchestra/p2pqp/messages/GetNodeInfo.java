package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class GetNodeInfo extends QpMessage {
	public final long origMsgId;
	public final InetSocketAddress origFrom;
	
	public GetNodeInfo(InetSocketAddress dest, InetSocketAddress origin) {
		super(dest);
		this.origMsgId = this.messageId;
		this.origFrom = origin;
	}
	
	public GetNodeInfo(InetSocketAddress dest, InetSocketAddress origin, long origMsgId) {
		super(dest);
		this.origMsgId = origMsgId;
		this.origFrom = origin;
	}

	private static final long serialVersionUID = 1L;

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeLong(origMsgId);
		buf.writeInetSocketAddress(origFrom);
	}
	
	public GetNodeInfo(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		origMsgId = buf.readLong();
		origFrom = buf.readInetSocketAddress();
	}
	
	public boolean retryable() {
		return true;
	}
}
