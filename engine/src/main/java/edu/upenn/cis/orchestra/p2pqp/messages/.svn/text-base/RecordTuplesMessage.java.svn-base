package edu.upenn.cis.orchestra.p2pqp.messages;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.p2pqp.IdRangeSet;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTupleKey;
import edu.upenn.cis.orchestra.p2pqp.RecordTuples;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.p2pqp.RecordTuples.OperatorAndPhase;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class RecordTuplesMessage extends QpMessage implements QueryOwnerMessage {
	private static final long serialVersionUID = 1L;
	public final Map<Integer,IdRangeSet> scanned;
	private final byte[] missing;
	public final Collection<Exception> exceptions;
	public final Collection<InetSocketAddress> failed;
	public final Collection<OperatorAndPhase> finished;
	private final int queryId;

	public RecordTuplesMessage(InetSocketAddress dest, Map<Integer,IdRangeSet> scanned, Collection<Exception> exceptions, Collection<InetSocketAddress> failedNodes,
			Map<OperatorAndPhase, List<QpTupleKey>> missing, Collection<OperatorAndPhase> finished, int queryId) {
		super(dest);

		ScratchOutputBuffer sob = new ScratchOutputBuffer();
		for (Map.Entry<RecordTuples.OperatorAndPhase, List<QpTupleKey>> me : missing.entrySet()) {
			RecordTuples.OperatorAndPhase oap = me.getKey();
			List<QpTupleKey> keys = me.getValue();
			sob.writeInt(oap.operator);
			sob.writeInt(oap.phase);
			sob.writeInt(keys.size());
			sob.writeInt(keys.get(0).getSchema().relId);
			for (QpTupleKey key : keys) {
				key.getBytes(sob);
			}
		}

		this.scanned = scanned;
		this.missing = sob.getData();
		this.queryId = queryId;
		this.failed = failedNodes;
		this.exceptions = exceptions;
		this.finished = finished;
	}

	public Map<OperatorAndPhase,? extends Collection<QpTupleKey>> getMissingKeys(QpSchema.Source schemas) {
		final int length = missing.length;
		if (length == 0) {
			return Collections.emptyMap();
		}
		ScratchInputBuffer sib = new ScratchInputBuffer(missing);
		Map<OperatorAndPhase,List<QpTupleKey>> retval = new HashMap<OperatorAndPhase,List<QpTupleKey>>();
		while (! sib.finished()) {
			final int operator = sib.readInt();
			final int phase = sib.readInt();
			int num = sib.readInt();
			final int relId = sib.readInt();
			final QpSchema schema = schemas.getSchema(relId);
			List<QpTupleKey> tuples = new ArrayList<QpTupleKey>(num);
			retval.put(new OperatorAndPhase(operator,phase), tuples);
			while (num > 0) {
				tuples.add(QpTupleKey.deserialize(schema, sib));
				--num;
			}
		}
		return retval;
	}

	public String toString() {
		return "RecordTuplesMessage(" + messageId + ")";
	}

	@Override
	protected
	void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(queryId);

		buf.writeInt(scanned.size());
		for (Map.Entry<Integer,IdRangeSet> me : scanned.entrySet()) {
			final int operator = me.getKey();
			final IdRangeSet ranges = me.getValue();
			buf.writeInt(operator);
			ranges.serialize(buf);
		}
		buf.writeBytes(missing);
		if (exceptions.isEmpty()) {
			buf.writeObject(null);
		} else {
			buf.writeObject(exceptions);
		}

		buf.writeInt(failed.size());
		for (InetSocketAddress node : failed) {
			buf.writeInetSocketAddress(node);
		}

		buf.writeInt(finished.size());
		for (OperatorAndPhase oap : finished) {
			buf.writeInt(oap.operator);
			buf.writeInt(oap.phase);
		}
	}

	@SuppressWarnings("unchecked")
	public RecordTuplesMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		queryId = buf.readInt();

		int numOperatorsAndPhases = buf.readInt();
		Map<Integer,IdRangeSet> scanned = new HashMap<Integer,IdRangeSet>();
		for (int i = 0; i < numOperatorsAndPhases; ++i) {
			int operator = buf.readInt();
			IdRangeSet ranges = IdRangeSet.deserialize(buf);
			scanned.put(operator, ranges);
		}
		this.scanned = Collections.unmodifiableMap(scanned);

		missing = buf.readBytes();
		Object o;
		try {
			o = buf.readObject();
		} catch (IOException e) {
			throw new SerializationException("Error deserializing exceptions", e);
		} catch (ClassNotFoundException e) {
			throw new SerializationException("Error deserializing exceptions", e);
		}
		if (o == null) {
			exceptions = Collections.emptyList();
		} else {
			exceptions = (List<Exception>) o;
		}
		int numFailedNodes = buf.readInt();
		if (numFailedNodes == 0) {
			failed = Collections.emptyList();
		} else {
			failed = new ArrayList<InetSocketAddress>(numFailedNodes);
			for (int i = 0; i < numFailedNodes; ++i) {
				failed.add(buf.readInetSocketAddress());
			}
		}

		int numFinishedOperators = buf.readInt();
		if (numFinishedOperators == 0) {
			finished = Collections.emptyList();
		} else {
			finished = new ArrayList<OperatorAndPhase>(numFinishedOperators);
			for (int i = 0; i < numFinishedOperators; ++i) {
				int operator = buf.readInt();
				int phase = buf.readInt();
				finished.add(new OperatorAndPhase(operator,phase));
			}
		}
	}


	public int getQueryId() {
		return queryId;
	}
}

