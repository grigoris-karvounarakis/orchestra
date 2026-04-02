package edu.upenn.cis.orchestra.p2pqp.messages;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.TupleStore;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class ConstraintViolationMsg extends QpMessage {
	private static final long serialVersionUID = 1L;

	private final byte[] tuplesBytes;
	private final int relId;
	private final List<TupleStore.ConstraintViolation> errors;

	public <M> ConstraintViolationMsg(InsertTuplesMessage itm, Map<QpTuple<M>,TupleStore.ConstraintViolation> errors) {
		super(itm, false);
		
		if (errors.isEmpty()) {
			throw new IllegalStateException("Should not being sending message about no tuples");
		}

		this.relId = itm.relId;
		ScratchOutputBuffer sob = new ScratchOutputBuffer();
		this.errors = new ArrayList<TupleStore.ConstraintViolation>(errors.size());
		for (Map.Entry<QpTuple<M>,TupleStore.ConstraintViolation> me : errors.entrySet()) {
			me.getKey().putStoreBytes(sob);
			this.errors.add(me.getValue());
		}
		this.tuplesBytes = sob.getData();
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeBytes(tuplesBytes);
		buf.writeObject(errors);
	}

	@SuppressWarnings("unchecked")
	public ConstraintViolationMsg(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);

		this.relId = buf.readInt();
		this.tuplesBytes = buf.readBytes();
		try {
			this.errors = (List<TupleStore.ConstraintViolation>) buf.readObject();
		} catch (IOException ioe) {
			throw new SerializationException("Error retrieving constraint violations", ioe);
		} catch (ClassNotFoundException e) {
			throw new SerializationException("Error retrieving constraint violations", e);
		}
	}

	public <M> Map<QpTuple<M>,TupleStore.ConstraintViolation> getErrorTuples(QpSchema.Source schemas) {
		QpSchema schema = schemas.getSchema(relId);
		Map<QpTuple<M>,TupleStore.ConstraintViolation> retval = new HashMap<QpTuple<M>,TupleStore.ConstraintViolation>(errors.size());
		ScratchInputBuffer in = new ScratchInputBuffer(tuplesBytes);
		Iterator<TupleStore.ConstraintViolation> it = errors.iterator();
		
		while (it.hasNext()) {
			QpTuple<M> t = QpTuple.fromStoreBytes(schema, in);
			retval.put(t, it.next());
		}
		return retval;
	}
}
