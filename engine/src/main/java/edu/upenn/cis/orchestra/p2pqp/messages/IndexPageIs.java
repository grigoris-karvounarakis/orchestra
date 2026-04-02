package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.IdRange;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class IndexPageIs extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final IdRange pageRange;
	public final int numKeys;
	private final ByteArrayWrapper data;

	public IndexPageIs(RequestIndexPage request, IdRange pageRange, int numKeys, ByteArrayWrapper data) {
		super(request, true);
		this.data = data;
		this.numKeys = numKeys;
		this.pageRange = pageRange;
	}

	public String toString() {
		return "IndexPageIs";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeBytes(data.array, data.offset, data.length);
		pageRange.serialize(buf);
		buf.writeInt(numKeys);
	}

	public IndexPageIs(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		byte[] data = buf.readBytes();
		this.data = new ByteArrayWrapper(data);
		pageRange = IdRange.deserialize(buf);
		numKeys = buf.readInt();
	}
	
	public ByteArrayWrapper getData() {
		return data;
	}
	
	public int getCompressedDataLength() {
		return data.length;
	}
}