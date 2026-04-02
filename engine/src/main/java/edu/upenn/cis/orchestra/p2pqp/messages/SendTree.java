package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class SendTree extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public final int relId, treeNo;
	public final byte[] data;
	
	public SendTree(InetSocketAddress dest, int relId, int treeNo, byte[] data) {
		super(dest);
		this.relId = relId;
		this.treeNo = treeNo;
		this.data = data;
	}
	
	public SendTree(GetTree request, byte[] treeContents) {
		super(request);
		this.relId = request.relId;
		this.treeNo = request.treeNo;
		this.data = treeContents;
	}
	
	public String toString() {
		return "SendTree(" + relId + "," + treeNo + ")";
	}
	
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(treeNo);
		buf.writeBytes(data);
	}
	
	public SendTree(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		this.relId = buf.readInt();
		this.treeNo = buf.readInt();
		this.data = buf.readBytes();
	}
}
