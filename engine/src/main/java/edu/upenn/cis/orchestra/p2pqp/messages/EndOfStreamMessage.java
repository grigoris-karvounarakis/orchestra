package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.LongList;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class EndOfStreamMessage extends QpMessage implements QueryExecutionMessage {
	private static final long serialVersionUID = 1L;
	private final String namedNodeDest;
	private final boolean distributedDest;
	private final int queryId;
	public final long[] msgIds;
	public final int phaseNo;
	public final int destOperator;
	public final WhichInput destInput;
	
	public EndOfStreamMessage(InetSocketAddress dest, String namedNodeDest, boolean distributedDest, int queryId, LongList msgIds, int phaseNo, int destOperator, WhichInput destInput) {
		super(dest);
		this.namedNodeDest = namedNodeDest;
		this.distributedDest = distributedDest;
		this.queryId = queryId;
		this.msgIds = msgIds.getList();
		this.phaseNo = phaseNo;
		this.destOperator = destOperator;
		this.destInput = destInput;
	}

	private static final WhichInput[] whichInputValues = WhichInput.values();
	
	public EndOfStreamMessage(InputBuffer buf, InetSocketAddress origin)
			throws SerializationException {
		super(buf, origin);
		namedNodeDest = buf.readString();
		distributedDest = buf.readBoolean();
		queryId = buf.readInt();
		int numMsgIds = buf.readInt();
		msgIds = new long[numMsgIds];
		for (int i = 0; i < numMsgIds; ++i) {
			msgIds[i] = buf.readLong();
		}
		phaseNo = buf.readInt();
		destOperator = buf.readInt();
		int destInputOrdinal = buf.readInt();
		if (destInputOrdinal < 0) {
			destInput = null;
		} else {
			destInput = whichInputValues[destInputOrdinal];
		}
	}

	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeString(namedNodeDest);
		buf.writeBoolean(distributedDest);
		buf.writeInt(queryId);
		buf.writeInt(msgIds.length);
		for (long msgId : msgIds) {
			buf.writeLong(msgId);
		}
		buf.writeInt(phaseNo);
		buf.writeInt(destOperator);
		if (destInput == null) {
			buf.writeInt(-1);
		} else {
			buf.writeInt(destInput.ordinal());
		}
	}

	@Override
	public boolean centralDest() {
		return (! distributedDest) && (namedNodeDest == null);
	}

	@Override
	public boolean distributedDest() {
		return distributedDest;
	}

	@Override
	public int getQueryId() {
		return queryId;
	}

	@Override
	public String namedDest() {
		return namedNodeDest;
	}
}
