package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class DummyMessage extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final boolean wantReply;
	private final byte[] payload;
	private Priority prio = Priority.NORMAL;
	
	public DummyMessage(InetSocketAddress dest, byte[] payload) {
		super(dest);
		this.wantReply = false;
		this.payload = payload;
	}
	
	public DummyMessage(InetSocketAddress dest) {
		this(dest,false);
	}

	public DummyMessage(InetSocketAddress dest, boolean wantReply) {
		super(dest);
		this.wantReply = wantReply;
		this.payload = null;
	}

	public DummyMessage(InetSocketAddress dest, Priority prio) {
		this(dest,false);
		this.prio = prio;
	}
	
	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeBoolean(wantReply);
		buf.writeBytes(payload);
	}

	public DummyMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		wantReply = buf.readBoolean();
		payload = buf.readBytes();
	}
	
	public Priority getPriority() {
		return prio;
	}
}
