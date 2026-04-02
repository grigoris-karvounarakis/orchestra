package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.NoSuchElementException;

import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTuple;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class InsertTuplesMessage extends DHTMessage {
	private static final long serialVersionUID = 1L;
	public final int relId;
	public final int epoch;
	private final byte[] tuples;

	public <M> InsertTuplesMessage(InetSocketAddress dest, int relId, int epoch, ScratchOutputBuffer tuples) {
		super(dest);

		if (tuples.length() == 0) {
			throw new IllegalArgumentException("Should not try to insert no tuples");
		}

		this.tuples = QuickLZ.compress(tuples.getData());
		this.relId = relId;
		this.epoch = epoch;
	}

	public <M> Iterator<QpTuple<M>> getTuples(final QpSchema.Source schemas) {
		return new Iterator<QpTuple<M>>() {
			final ScratchInputBuffer sib = new ScratchInputBuffer(QuickLZ.decompress(tuples));
			final QpSchema schema = schemas.getSchema(relId);
			QpTuple<M> t = null;

			@Override
			public boolean hasNext() {
				return (! sib.finished());
			}

			@Override
			public QpTuple<M> next() {
				if (sib.finished()) {
					throw new NoSuchElementException();
				}
				if (t == null) {
					t = QpTuple.fromStoreBytes(schema, sib);
				} else {
					t.changeDataFromStoreBytes(sib);
				}
				return t;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}

	public String toString() {
		return "InsertTuples";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relId);
		buf.writeInt(epoch);
		buf.writeBytes(tuples);
	}

	public InsertTuplesMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		relId = buf.readInt();
		epoch = buf.readInt();
		tuples = buf.readBytes();
	}
}

