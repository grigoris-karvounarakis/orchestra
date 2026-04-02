package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpApplication.NodeInfoWithLiveness;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class SendKnownNodes extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final Collection<NodeInfoWithLiveness> info;

	
	public SendKnownNodes(InetSocketAddress dest, Collection<NodeInfoWithLiveness> info) {
		super(dest);
		this.info = info;
	}

	public SendKnownNodes(InputBuffer buf, InetSocketAddress origin)
			throws SerializationException {
		super(buf, origin);
		int numNodes = buf.readInt();
		info = new ArrayList<NodeInfoWithLiveness>(numNodes);
		for (int i = 0; i < numNodes; ++i) {
			info.add(NodeInfoWithLiveness.deserialize(buf));
		}
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(info.size());
		for (NodeInfoWithLiveness niwl : info) {
			niwl.serialize(buf);
		}
	}

}
