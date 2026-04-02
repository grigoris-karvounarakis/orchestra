package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.List;

import edu.upenn.cis.orchestra.p2pqp.DHTService.EpochNumAndId;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class SendIndexPages extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public final int relId, epoch;
	public final byte[] data;

	public SendIndexPages(InetSocketAddress dest, int relId, int epoch, List<EpochNumAndId> pages, int numTuples, int treeNum) {
		super(dest);
		this.relId = relId;
		this.epoch = epoch;
		ScratchOutputBuffer out = new ScratchOutputBuffer();
		out.writeInt(numTuples);
		out.writeInt(treeNum);
		for (EpochNumAndId page : pages) {
			page.serialize(out);
		}
		data = out.getData();
	}

	public String toString() {
		return "SendIndexPages (" + relId + "," + epoch + ")";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeBytes(data);
	}

	public SendIndexPages(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		data = buf.readBytes();
	}
}
