package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTupleKey;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class RemoveTuplesMessage extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public final int relId;
	private final byte[] tuples;

	public RemoveTuplesMessage(InetSocketAddress dest, int relId, ScratchOutputBuffer ts) {
		super(dest);
		this.relId = relId;
		this.tuples = ts.getData();
	}

	public List<QpTupleKey> getKeys(QpSchema.Source ss) {
		final QpSchema schema = ss.getSchema(relId);
		ScratchInputBuffer sib = new ScratchInputBuffer(tuples);
		ArrayList<QpTupleKey> retval = new ArrayList<QpTupleKey>();
		while (! sib.finished()) {
			retval.add(QpTupleKey.deserialize(schema, sib));
		}
		return retval;
	}

	public String toString() {
		return "RemoveTuple";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeBytes(tuples);
	}

	public RemoveTuplesMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		tuples = buf.readBytes();
	}
}
