package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;

import edu.upenn.cis.orchestra.p2pqp.MetadataFactory;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTupleBag;
import edu.upenn.cis.orchestra.p2pqp.Operator.WhichInput;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class ShippedTuplesMessage extends QpMessage implements QueryExecutionMessage {
	private static final long serialVersionUID = 1L;
	public final int destOperator;
	public final WhichInput whichChild;
	private final int relId;
	private final boolean zipCompressed;
	private final boolean quickLzCompressed;
	private final byte[] data;
	private final int dataOffset, dataLength;
	private final String namedNodeDest;
	private final boolean distributedDest;
	private final int queryId;
	
	public <M> ShippedTuplesMessage(InetSocketAddress dest, String namedNodeDest, boolean distributedDest, int queryId, QpTupleBag<M> tuples, int destOperator,
			WhichInput whichChild, int compressionLevel) {
		super(dest);
		this.queryId = queryId;
		this.namedNodeDest = namedNodeDest;
		this.distributedDest = distributedDest;
		this.relId = tuples.schema.relId;
		ByteArrayWrapper data = tuples.getBytes();
		if (compressionLevel == Deflater.NO_COMPRESSION) {
			this.data = data.array;
			this.dataOffset = data.offset;
			this.dataLength = data.length;
			zipCompressed = false;
			quickLzCompressed = false;
		} else if (compressionLevel < 0) {
			byte[] dataToCompress;
			if (data.offset == 0 && data.length == data.array.length) {
				dataToCompress = data.array;
			} else {
				dataToCompress = new byte[data.length];
				System.arraycopy(data.array, data.offset, dataToCompress, 0, data.length);
			}
			this.data = QuickLZ.compress(dataToCompress);
			dataOffset = 0;
			dataLength = this.data.length;
			quickLzCompressed = true;
			zipCompressed = false;
		} else {
			this.data = compressors.get().compress(data.array, data.offset, data.length, compressionLevel);
			this.dataOffset = 0;
			this.dataLength = this.data.length;
			zipCompressed = true;
			quickLzCompressed = false;
		}
		this.destOperator = destOperator;
		this.whichChild = whichChild;
	}

	public <M> QpTupleBag<M> getTuples(QpSchema.Source schemas, MetadataFactory<M> mdf) {
		final byte[] data;
		final int offset, length;
		if (quickLzCompressed) {
			if (this.dataOffset == 0 && this.dataLength == this.data.length) {
				data = QuickLZ.decompress(this.data);				
			} else {
				byte[] toDecompress = new byte[this.dataLength];
				System.arraycopy(this.data, this.dataOffset, toDecompress, 0, this.dataLength);
				data = QuickLZ.decompress(toDecompress);
			}

			offset = 0;
			length = data.length;
		} else if (zipCompressed) {
			try {
				data = compressors.get().decompress(this.data, this.dataOffset, this.dataLength);
			} catch (DataFormatException e) {
				throw new RuntimeException("Error decompressing shipped tuples data", e);
			}
			offset = 0;
			length = data.length;
		} else {
			data = this.data;
			offset = this.dataOffset;
			length = this.dataLength;
		}
		QpSchema schema = schemas.getSchema(relId);
		return new QpTupleBag<M>(schema, schemas, mdf, data, offset, length);
	}
	
	public long getDataLength() {
		return this.data.length;
	}

	public String toString() {
		return "ShippedTuplesMessage";
	}

	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(destOperator);
		if (whichChild == null) {
			buf.writeInt(-1);
		} else {
			buf.writeInt(whichChild.ordinal());
		}
		buf.writeInt(relId);
		buf.writeBoolean(this.zipCompressed);
		buf.writeBoolean(this.quickLzCompressed);
		buf.writeBytes(data, dataOffset, dataLength);

		buf.writeString(namedNodeDest);
		buf.writeBoolean(distributedDest);
		buf.writeInt(queryId);
	}
	
	private static final WhichInput[] whichInputValues = WhichInput.values();

	public ShippedTuplesMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		destOperator = buf.readInt();
		int whichChildOrdinal = buf.readInt();
		if (whichChildOrdinal < 0) {
			whichChild = null;
		} else {
			whichChild = whichInputValues[whichChildOrdinal];
		}
		relId = buf.readInt();
		zipCompressed = buf.readBoolean();
		quickLzCompressed = buf.readBoolean();
		data = buf.readBytes();
		dataOffset = 0;
		dataLength = data.length;
		namedNodeDest = buf.readString();
		distributedDest = buf.readBoolean();
		queryId = buf.readInt();
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