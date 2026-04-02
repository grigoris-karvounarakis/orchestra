package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTupleKey;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class MissingTuplesAre extends QpMessage {
	private static final long serialVersionUID = 1L;
	private final byte[] missingTuples;

	public MissingTuplesAre(CheckRelation cr, Collection<QpTupleKey> missing) {
		super(cr);

		ScratchOutputBuffer sob = new ScratchOutputBuffer();
		for (QpTupleKey t : missing) {
			t.getBytes(sob);
		}

		missingTuples = sob.getData();
	}

	public MissingTuplesAre(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		missingTuples = buf.readBytes();
	}

	@Override
	protected
	void subclassSerialize(OutputBuffer buf) {
		buf.writeBytes(missingTuples);
	}

	public Set<QpTupleKey> getData(QpSchema schema) {
		Set<QpTupleKey> retval = new HashSet<QpTupleKey>();
		ScratchInputBuffer sib = new ScratchInputBuffer(missingTuples);
		while (! sib.finished()) {
			retval.add(QpTupleKey.deserialize(schema, sib));
		}
		return retval;
	}
}
