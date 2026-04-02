package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;

public abstract class DHTMessage extends QpMessage {
	DHTMessage(Id dest) {
		super(dest);
	}

	DHTMessage(InetSocketAddress dest) {
		super(dest);
	}
	
	DHTMessage(QpMessage m) {
		super(m);
	}

	DHTMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
	}
	private static final long serialVersionUID = 1L;
}

