package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import edu.upenn.cis.orchestra.p2pqp.IdRangeSet;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.Router;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class BeginNewQueryPhase extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int queryId;
	public final int phaseNo;
	public final Router router;
	public final IdRangeSet previousPhaseFailedRanges;
	public final Collection<InetSocketAddress> newlyFailedNodes;

	public BeginNewQueryPhase(InetSocketAddress dest, int queryId, int phaseNo, Router newRouter,
			IdRangeSet previousPhaseFailedRanges, Collection<InetSocketAddress> newlyFailedNodes) {
		super(dest);
		this.queryId = queryId;
		this.phaseNo = phaseNo;
		this.router = newRouter;
		this.previousPhaseFailedRanges = previousPhaseFailedRanges.clone();
		this.newlyFailedNodes = newlyFailedNodes;
	}

	public BeginNewQueryPhase(InputBuffer buf, InetSocketAddress origin)
	throws SerializationException {
		super(buf, origin);
		queryId = buf.readInt();
		phaseNo = buf.readInt();
		router = Router.deserialize(buf);
		previousPhaseFailedRanges = IdRangeSet.deserialize(buf);
		int numNodes = buf.readInt();
		newlyFailedNodes = new ArrayList<InetSocketAddress>(numNodes);
		for (int i = 0; i < numNodes; ++i) {
			newlyFailedNodes.add(buf.readInetSocketAddress());
		}
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(queryId);
		buf.writeInt(phaseNo);
		router.serialize(buf);
		previousPhaseFailedRanges.serialize(buf);
		buf.writeInt(newlyFailedNodes.size());
		for (InetSocketAddress node : newlyFailedNodes) {
			buf.writeInetSocketAddress(node);
		}
	}
}
