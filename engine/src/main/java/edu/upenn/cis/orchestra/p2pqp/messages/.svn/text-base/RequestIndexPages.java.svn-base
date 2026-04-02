package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class RequestIndexPages extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public final int relId, epoch;
	public final boolean wantNonRedirect;

	public RequestIndexPages(Id dest, int relId, int epoch, boolean wantNonRedirect) {
		super(dest);
		this.relId = relId;
		this.epoch = epoch;
		this.wantNonRedirect = wantNonRedirect;
	}

	public String toString() {
		return "RequestIndexPages (" + relId + "," + epoch + ")";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeBoolean(wantNonRedirect);
	}


	public RequestIndexPages(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		wantNonRedirect = buf.readBoolean();
	}
}
