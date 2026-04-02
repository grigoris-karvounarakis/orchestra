package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.DataFormatException;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.p2pqp.IdRangeSet;
import edu.upenn.cis.orchestra.p2pqp.KeySource;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTupleKey;
import edu.upenn.cis.orchestra.p2pqp.DHTService.EpochNum;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.p2pqp.QpSchema.Source;
import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;

public class ScanTuplesMessage extends QpMessage implements QueryExecutionMessage, KeySource {
	private static final long serialVersionUID = 1L;
	private final int queryId;
	private final int relId;
	private final boolean zipCompressed;
	private final boolean quickLzCompressed;
	private final byte[] data;
	private final int offset, length;
	public final int operatorId;
	private final int epoch;
	private final int pageNum;
	public final int phaseNo;
	public final IdRangeSet keyRanges;
	public final int numKeys;
		
	public ScanTuplesMessage(InetSocketAddress dest, int relId, int phaseNo, int queryId, int operatorId, byte[] data, int offset, int length, int epoch, int pageNum, IdRangeSet keyRanges, int numKeys, int compressionLevel) {
		super(dest);
		this.queryId = queryId;
		if (length == 0) {
			this.data = null;
			this.offset = -1;
			this.length = 0;
			zipCompressed = false;
			quickLzCompressed = false;
		} else if (compressionLevel == 0) {
			zipCompressed = false;
			quickLzCompressed = false;
			this.data = data;
			this.offset = offset;
			this.length = length;
		} else if (compressionLevel < 0) {
			zipCompressed = false;
			quickLzCompressed = true;
			byte[] toCompress;
			if (offset == 0 && length == data.length) {
				toCompress = data;
			} else {
				toCompress = new byte[length];
				System.arraycopy(data, offset, toCompress, 0, length);
			}
			this.data = QuickLZ.compress(toCompress);
			this.offset = 0;
			this.length = this.data.length;
		} else {
			zipCompressed = true;
			quickLzCompressed = false;
			this.data = compressors.get().compress(data, offset, length, compressionLevel);
			this.offset = 0;
			this.length = this.data.length;
		}
		this.operatorId = operatorId;
		this.relId = relId;
		this.epoch = epoch;
		this.pageNum = pageNum;
		this.phaseNo = phaseNo;
		this.keyRanges = keyRanges;
		this.numKeys = numKeys;
		
		if (keyRanges.isEmpty()) {
			throw new IllegalArgumentException("Key range should not be empty");
		}
	}

	public String toString() {
		return "ScanTuplesMessage";
	}

	@Override
	protected
	void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(queryId);
		buf.writeInt(relId);
		buf.writeBytes(data, offset, length);
		buf.writeInt(operatorId);
		buf.writeInt(epoch);
		buf.writeInt(pageNum);
		buf.writeInt(phaseNo);
		buf.writeInt(numKeys);
		buf.writeBoolean(zipCompressed);
		buf.writeBoolean(quickLzCompressed);
		keyRanges.serialize(buf);
	}

	public ScanTuplesMessage(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf, origin);
		queryId = buf.readInt();
		relId = buf.readInt();
		data = buf.readBytes();
		offset = 0;
		length = data.length;
		operatorId = buf.readInt();
		epoch = buf.readInt();
		pageNum = buf.readInt();
		phaseNo = buf.readInt();
		numKeys = buf.readInt();
		zipCompressed = buf.readBoolean();
		quickLzCompressed = buf.readBoolean();
		keyRanges = IdRangeSet.deserialize(buf);
	}

	public int getQueryId() {
		return queryId;
	}
	
	public EpochNum getEpochNum() {
		return new EpochNum(epoch, pageNum);
	}

	@Override
	public int getNumKeys() {
		return numKeys;
	}
	
	@Override
	public Iterator<QpTupleKey> getKeys(Source findSchema) {
		if (length == 0) {
			List<QpTupleKey> noKeys = Collections.emptyList();
			return noKeys.iterator();
		}
		final ScratchInputBuffer sib;
		if (zipCompressed) {
			try {
				sib = new ScratchInputBuffer(compressors.get().decompress(this.data, this.offset, this.length));
			} catch (DataFormatException e) {
				throw new RuntimeException("Error decompressing key data", e);
			}
		} else if (quickLzCompressed) {
			sib = new ScratchInputBuffer(QuickLZ.decompress(this.data));
		} else {
			sib = new ScratchInputBuffer(data, offset, length);
		}
		final QpSchema schema = findSchema.getSchema(relId);
		
		return new Iterator<QpTupleKey>() {

			@Override
			public boolean hasNext() {
				return (! sib.finished());
			}

			@Override
			public QpTupleKey next() {
				if (sib.finished()) {
					throw new NoSuchElementException();
				}
				return QpTupleKey.deserialize(schema, sib);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}


	@Override
	public boolean centralDest() {
		return false;
	}

	@Override
	public boolean distributedDest() {
		return true;
	}

	@Override
	public String namedDest() {
		return null;
	}

	@Override
	public void addKeysTo(ByteArraySet set) {
		byte[] data;
		int offset, length;
		if (zipCompressed) {
			try {
				data = compressors.get().decompress(this.data, this.offset, this.length);
				offset = 0;
				length = data.length;
			} catch (DataFormatException e) {
				throw new RuntimeException("Error decompressing key data", e);
			}
		} else if (quickLzCompressed) {
			data = QuickLZ.decompress(this.data);
			offset = 0;
			length = data.length;
		} else {
			data = this.data;
			offset = this.offset;
			length = this.length;
		}
		int pos = offset;
		final int end = offset + length;
		while (pos < end) {
			int keyLength = IntType.getValFromBytes(data, pos);
			pos += IntType.bytesPerInt;
			set.add(data, pos, keyLength);
			pos += keyLength;
		}
	}
}
