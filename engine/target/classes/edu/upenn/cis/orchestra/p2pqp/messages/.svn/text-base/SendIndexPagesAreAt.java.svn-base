package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class SendIndexPagesAreAt extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public  final int relId, epoch, locEpoch;

	public SendIndexPagesAreAt(Id dest, int relId, int epoch, int locEpoch) {
		super(dest);
		this.relId = relId;
		this.epoch = epoch;
		this.locEpoch = locEpoch;
	}

	public String toString() {
		return "SendIndexPagesAreAt (" + relId + "," + epoch + ") ->" + locEpoch;
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeInt(locEpoch);
	}

	public SendIndexPagesAreAt(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		locEpoch = buf.readInt();
	}
}
