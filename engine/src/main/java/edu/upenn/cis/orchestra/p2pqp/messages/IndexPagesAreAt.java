package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class IndexPagesAreAt extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int epoch;

	public IndexPagesAreAt(RequestIndexPages request, int epoch) {
		super(request, true);
		this.epoch = epoch;
	}

	public String toString() {
		return "IndexPagesAreAt (" + epoch + ")";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(epoch);
	}

	public IndexPagesAreAt(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		epoch = buf.readInt();
	}
}
