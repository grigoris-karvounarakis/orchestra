package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.List;

import edu.upenn.cis.orchestra.p2pqp.Id;
import edu.upenn.cis.orchestra.p2pqp.IdRangeSet;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class RequestSendKeys extends DHTMessage {
	private static final long serialVersionUID = 1L;

	public final int relId;
	public final int epoch, number, queryId, operatorId;
	public final int phaseNo;

	public final byte[] keyPredBytes;
	public final int compressionLevel;
	public final InetSocketAddress dests[];
	public final IdRangeSet[] destRangesToSend;

	public RequestSendKeys(Id dest, List<InetSocketAddress> pageDests, List<IdRangeSet> destRangesToSend, int relId, int epoch, int number, int queryId, int operatorId, byte[] keyFilterBytes, int phaseNo, int compressionLevel) {
		super(dest);
		this.relId = relId;
		this.epoch = epoch;
		this.number = number;
		this.queryId = queryId;
		this.operatorId = operatorId;
		this.phaseNo = phaseNo;

		this.keyPredBytes = keyFilterBytes;
		this.dests = pageDests.toArray(new InetSocketAddress[pageDests.size()]);
		this.destRangesToSend = destRangesToSend.toArray(new IdRangeSet[destRangesToSend.size()]);
		if (dests.length != this.destRangesToSend.length) {
			throw new IllegalArgumentException("Lists of page dests and ranges to send must have the same length");
		}
		for (int i = 0; i < dests.length; ++i) {
			if (this.destRangesToSend[i].isEmpty()) {
				throw new IllegalArgumentException("Empty range for relation " + relId + " page (" + epoch + "," + number + ") at node " + dests[i]);
			}
		}
		this.compressionLevel = compressionLevel;
	}

	public String toString() {
		return "RequestSendKeys(" + relId + "," + epoch + "," + number + "," + queryId + "," + operatorId + "," + phaseNo + ")";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeInt(number);
		buf.writeInt(queryId);
		buf.writeInt(operatorId);
		buf.writeInt(phaseNo);

		buf.writeBytes(keyPredBytes);
		buf.writeInt(dests.length);
		for (InetSocketAddress dest : dests) {
			buf.writeInetSocketAddress(dest);
		}
		for (IdRangeSet ranges : destRangesToSend) {
			ranges.serialize(buf);
		}
		buf.writeInt(compressionLevel);
	}

	public RequestSendKeys(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		number = buf.readInt();
		queryId = buf.readInt();
		operatorId = buf.readInt();
		phaseNo = buf.readInt();

		keyPredBytes = buf.readBytes();

		int numDests = buf.readInt();
		dests = new InetSocketAddress[numDests];
		for (int i = 0; i < numDests; ++i) {
			dests[i] = buf.readInetSocketAddress();
		}
		destRangesToSend = new IdRangeSet[numDests];
		for (int i = 0; i < numDests; ++i) {
			destRangesToSend[i] = IdRangeSet.deserialize(buf);
		}
		compressionLevel = buf.readInt();
	}

	public Priority getPriority() {
		return Priority.HIGH;
	}
	
	public boolean retryable() {
		return true;
	}
}
