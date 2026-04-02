package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class RequestIndexPage extends DHTMessage {
	private static final long serialVersionUID = 1L;

	public final int relId, epoch, number;

	public RequestIndexPage(Id dest, int relId, int epoch, int number) {
		super(dest);
		this.relId = relId;
		this.epoch = epoch;
		this.number = number;
	}

	public String toString() {
		return "RequestIndexPage (" + relId + "," + epoch + "," + number + ")";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeInt(number);
	}

	public RequestIndexPage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		number = buf.readInt();
	}
}
