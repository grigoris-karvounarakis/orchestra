package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.Router;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class NodeInfoIs extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final Router.NodeInfo ni;
	
	public NodeInfoIs(GetNodeInfo gri, Id id, InetSocketAddress qpAddress) {
		super(gri.origFrom, new long[] {gri.origMsgId});
		this.ni = new Router.NodeInfo(id, qpAddress);
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		ni.serialize(buf);
	}

	public NodeInfoIs(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		ni = Router.NodeInfo.deserialize(buf);
	}
	
	public String toString() {
		return "NodeInfoIs(" + this.messageId + ")";
	}
}
