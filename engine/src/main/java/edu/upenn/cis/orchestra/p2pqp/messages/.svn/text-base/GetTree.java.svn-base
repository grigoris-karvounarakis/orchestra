package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class GetTree extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public final int relId, treeNo;
	
	public GetTree(Id dest, int relId, int treeNo) {
		super(dest);
		this.relId = relId;
		this.treeNo = treeNo;
	}
	
	public String toString() {
		return "GetTree(" + relId + "," + treeNo + ")";
	}
	
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(treeNo);
	}
	
	public GetTree(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		this.relId = buf.readInt();
		this.treeNo = buf.readInt();
	}
}
