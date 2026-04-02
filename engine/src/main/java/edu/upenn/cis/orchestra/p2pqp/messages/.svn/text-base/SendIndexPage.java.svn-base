package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.IdRange;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class SendIndexPage extends DHTMessage {
	private static final long serialVersionUID = 1L;
	private final byte[] data;
	public final int relId, epoch, number;
	public final IdRange pageRange;
	public final int numKeys;
	

	public SendIndexPage(InetSocketAddress dest, int relId, int epoch, int number, IdRange pageRange, byte[] data, int numKeys) {
		super(dest);
		this.data = QuickLZ.compress(data);
		this.relId = relId;
		this.epoch = epoch;
		this.number = number;
		this.pageRange = pageRange;
		this.numKeys = numKeys;
	}

	public String toString() {
		return "SendIndexPage (" + relId + "," + epoch + "," + number + ")";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeBytes(data);
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeInt(number);
		pageRange.serialize(buf);
		buf.writeInt(numKeys);
	}

	public SendIndexPage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		data = buf.readBytes();
		relId = buf.readInt();
		epoch = buf.readInt();
		number = buf.readInt();
		pageRange = IdRange.deserialize(buf);
		numKeys = buf.readInt();
	}
	
	public byte[] getData() {
		return QuickLZ.decompress(data);
	}
}