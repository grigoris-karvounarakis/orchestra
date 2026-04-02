package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ConnectMessage extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final Id senderId;
	
	public ConnectMessage(InetSocketAddress dest, Id senderId) {
		super(dest);
		this.senderId = senderId;
	}

	public ConnectMessage(InputBuffer buf, InetSocketAddress origin)
			throws SerializationException {
		super(buf, origin);
		senderId = Id.deserialize(buf);
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		senderId.serialize(buf);
	}

}
