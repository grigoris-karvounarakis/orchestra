package edu.upenn.cis.orchestra.p2pqp;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.exceptions.CompareMismatch;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.WriteableByteArray;

public class QpTuple<M> extends AbstractImmutableTuple<QpSchema> implements Comparable<QpTuple<?>> {
	public static final int FIRST_EPOCH = 0;
	private static final long serialVersionUID = 1L;

	/*
	 * Serialization format:
	 * 		phase #
	 * 		# of contributing nodes
	 * 		InetSocketAddress for each contributing node
	 * 		metadata length
	 * 		metadata bytes
	 * 
	 * AbstractImmutableTuple serialization
	 */

	private int phaseNo;
	private byte[] contributingNodes;
	private int contributingNodesOffset, contributingNodesLength;
	private byte[] metadata;
	private int metadataOffset, metadataLength;

	public QpTuple(AbstractTuple<QpSchema> t) {
		super(t,false);
		phaseNo = -1;
		contributingNodes = null;
		contributingNodesOffset = -1;
		contributingNodesLength = -1;
		metadata = null;
		metadataOffset = -1;
		metadataLength = -1;
	}

	public QpTuple(AbstractImmutableTuple<QpSchema> t) {
		super(t,false);
		phaseNo = -1;
		contributingNodes = null;
		contributingNodesOffset = -1;
		contributingNodesLength = -1;
		metadata = null;
		metadataOffset = -1;
		metadataLength = -1;
	}

	private static byte[] init(InetSocketAddress[] nodes) {
		byte[] retval = new byte[OutputBuffer.inetSocketAddressLen * nodes.length];
		int pos = 0;
		for (InetSocketAddress node : nodes) {
			OutputBuffer.getBytes(node, retval, pos);
			pos += OutputBuffer.inetSocketAddressLen;
		}

		return retval;
	}

	private static byte[] init(Set<InetSocketAddress> nodes) {
		byte[] retval = new byte[OutputBuffer.inetSocketAddressLen * nodes.size()];
		int pos = 0;
		for (InetSocketAddress node : nodes) {
			OutputBuffer.getBytes(node, retval, pos);
			pos += OutputBuffer.inetSocketAddressLen;
		}

		return retval;
	}
	
	public QpTuple(QpSchema schema, InetSocketAddress[] contributingNodes, Object[] fields, int phaseNo) throws ValueMismatchException {
		this(schema,contributingNodes,null,null,fields,phaseNo);
	}

	public QpTuple(QpSchema schema, InetSocketAddress[] contributingNodes, M metadata, MetadataFactory<M> mdf, Object[] fields) throws ValueMismatchException {
		this(schema,contributingNodes,metadata,mdf,fields,0);
	}

	public QpTuple(QpSchema schema, InetSocketAddress[] contributingNodes, M metadata, MetadataFactory<M> mdf, Object[] fields, int phaseNo) throws ValueMismatchException {
		super(schema,false,fields);
		if (contributingNodes == null) {
			this.contributingNodes = null;
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodes = init(contributingNodes);
			this.contributingNodesOffset = 0;
			this.contributingNodesLength = this.contributingNodes.length;
		}
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		this.phaseNo = phaseNo;
	}

	public QpTuple(QpSchema schema, int phaseNo, Object[] fields) throws ValueMismatchException {
		super(schema, false, fields);
		this.contributingNodes = null;
		this.contributingNodesOffset = -1;
		this.contributingNodesLength = -1;
		this.metadata = null;
		this.metadataOffset = -1;
		this.metadataLength = -1;
		this.phaseNo = phaseNo;
	}
	
	public QpTuple(QpSchema schema, Object[] fields) throws ValueMismatchException {
		this(schema, -1, fields);
	}

	public QpTuple(QpSchema schema, Object[] fields, QpTuple<M> forMetadata) throws ValueMismatchException {
		super(schema,false,fields);
		this.contributingNodes = forMetadata.contributingNodes;
		this.contributingNodesOffset = forMetadata.contributingNodesOffset;
		this.contributingNodesLength = forMetadata.contributingNodesLength;
		this.metadata = forMetadata.metadata;
		this.metadataOffset = forMetadata.metadataOffset;
		this.metadataLength = forMetadata.metadataLength;
		this.phaseNo = forMetadata.phaseNo;
	}

	public QpTuple(QpSchema schema, AbstractRelation.FieldSource[] fss, QpTuple<M> t, Object[] otherFields) throws ValueMismatchException {
		super(schema,fss,t,otherFields);
		this.contributingNodes = t.contributingNodes;
		this.contributingNodesOffset = t.contributingNodesOffset;
		this.contributingNodesLength = t.contributingNodesLength;
		this.metadata = t.metadata;
		this.metadataOffset = t.metadataOffset;
		this.metadataLength = t.metadataLength;
		this.phaseNo = t.phaseNo;
	}

	public QpTuple(QpSchema schema, AbstractRelation.FieldSource[] fss, QpTuple<?> t, M metadata, MetadataFactory<M> mdf, Object[] otherFields) throws ValueMismatchException {
		super(schema,fss,t,otherFields);
		this.contributingNodes = t.contributingNodes;
		this.contributingNodesOffset = t.contributingNodesOffset;
		this.contributingNodesLength = t.contributingNodesLength;

		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}

		this.phaseNo =  t.phaseNo;
	}

	public QpTuple(QpSchema schema, QpTuple<?> tuple, AbstractRelation.RelationMapping oneTupleMapping, InetSocketAddress[] contributingNodes, M metadata, MetadataFactory<M> mdf) {
		super(schema, tuple, false, oneTupleMapping);
		this.phaseNo = tuple.phaseNo;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		if (contributingNodes == null) {
			this.contributingNodes = null;
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodes = init(contributingNodes);
			this.contributingNodesOffset = 0;
			this.contributingNodesLength = this.contributingNodes.length;
		}
	}

	public QpTuple(QpSchema schema, QpTuple<?> tuple, boolean onlyKey, AbstractRelation.RelationMapping oneTupleMapping, M metadata, MetadataFactory<M> mdf) {
		super(schema, tuple, onlyKey, oneTupleMapping);
		this.contributingNodes = tuple.contributingNodes;
		this.contributingNodesOffset = tuple.contributingNodesOffset;
		this.contributingNodesLength = tuple.contributingNodesLength;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		this.phaseNo = tuple.phaseNo;
	}

	public QpTuple(QpSchema schema, QpTuple<M> t, AbstractRelation.RelationMapping oneTupleMapping) {
		super(schema, t, false, oneTupleMapping);
		this.contributingNodes = t.contributingNodes;
		this.contributingNodesOffset = t.contributingNodesOffset;
		this.contributingNodesLength = t.contributingNodesLength;
		this.metadata = t.metadata;
		this.metadataOffset = t.metadataOffset;
		this.metadataLength = t.metadataLength;
		this.phaseNo = t.phaseNo;
	}

	public QpTuple(QpSchema schema, QpTuple<?> t, AbstractRelation.RelationMapping oneTupleMapping, boolean clear) {
		super(schema, t, false, oneTupleMapping);
		this.contributingNodes = null;
		this.contributingNodesOffset = -1;
		this.contributingNodesLength = -1;
		this.metadata = null;
		this.metadataOffset = -1;
		this.metadataLength = -1;
		this.phaseNo = -1;
	}

	public QpTuple(QpTuple<?> tuple, AbstractRelation.RelationMapping retainColsMapping, InetSocketAddress[] contributingNodes, M metadata, MetadataFactory<M> mdf) {
		super(tuple.schema, tuple, false, retainColsMapping);
		this.phaseNo = tuple.phaseNo;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		if (contributingNodes == null) {
			this.contributingNodes = null;
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodes = init(contributingNodes);
			this.contributingNodesOffset = 0;
			this.contributingNodesLength = this.contributingNodes.length;
		}
	}


	public QpTuple(QpTuple<?> tuple, AbstractRelation.RelationMapping retainColsMapping, M metadata, MetadataFactory<M> mdf) {
		super(tuple.schema, tuple, false, retainColsMapping);
		this.phaseNo = tuple.phaseNo;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		if (tuple.contributingNodes == null) {
			this.contributingNodes = null;
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodes = tuple.contributingNodes;
			this.contributingNodesOffset = tuple.contributingNodesOffset;
			this.contributingNodesLength = tuple.contributingNodesLength;
		}
	}
	public QpTuple(QpSchema schema, QpTuple<?> tuple, AbstractRelation.RelationMapping oneTupleMapping, InetSocketAddress[] contributingNodes, M metadata, MetadataFactory<M> mdf, int phaseNo) {
		super(schema, tuple, false, oneTupleMapping);
		this.phaseNo = phaseNo;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		if (contributingNodes == null) {
			this.contributingNodes = null;
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodes = init(contributingNodes);
			this.contributingNodesOffset = 0;
			this.contributingNodesLength = this.contributingNodes.length;
		}
	}
	
	public QpTuple(QpSchema schema, QpTuple<?> tuple, AbstractRelation.RelationMapping oneTupleMapping, Set<InetSocketAddress> contributingNodes, M metadata, MetadataFactory<M> mdf, int phaseNo) {
		super(schema, tuple, false, oneTupleMapping);
		this.phaseNo = phaseNo;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}
		if (contributingNodes == null) {
			this.contributingNodes = null;
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodes = init(contributingNodes);
			this.contributingNodesOffset = 0;
			this.contributingNodesLength = this.contributingNodes.length;
		}
	}
	
	public QpTuple(QpTuple<M> t, AbstractRelation.RelationMapping retainColsMapping) {
		super(t, false, retainColsMapping);
		this.contributingNodes = t.contributingNodes;
		this.contributingNodesOffset = t.contributingNodesOffset;
		this.contributingNodesLength = t.contributingNodesLength;
		this.metadata = t.metadata;
		this.metadataOffset = t.metadataOffset;
		this.metadataLength = t.metadataLength;
		this.phaseNo = t.phaseNo;		
	}

	public QpTuple(AbstractTuple<QpSchema> tuple, M metadata, MetadataFactory<M> mdf) {
		super(tuple, false);
		this.contributingNodes = null;
		this.contributingNodesOffset = -1;
		this.contributingNodesLength = -1;
		this.phaseNo = -1;
		this.metadata = mdf.toBytes(metadata);
		this.metadataOffset = 0;
		this.metadataLength = this.metadata.length;
	}

	public QpTuple(AbstractImmutableTuple<QpSchema> tuple, M metadata, MetadataFactory<M> mdf) {
		super(tuple, false);
		this.contributingNodes = null;
		this.contributingNodesOffset = -1;
		this.contributingNodesLength = -1;
		this.phaseNo = -1;
		this.metadata = mdf.toBytes(metadata);
		this.metadataOffset = 0;
		this.metadataLength = this.metadata.length;
	}

	public QpTuple(QpSchema schema, AbstractRelation.RelationMapping joinMapping, QpTuple<?> first, QpTuple<?> second, M metadata, MetadataFactory<M> mdf, int phaseNo) {
		super(schema, joinMapping, first,second);
		this.phaseNo = phaseNo;
		if (metadata == null) {
			this.metadata = null;
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadata = mdf.toBytes(metadata);
			this.metadataOffset = 0;
			this.metadataLength = this.metadata.length;
		}

		if (first.contributingNodes == null || second.contributingNodes == null) {
			contributingNodes = null;
			contributingNodesOffset = -1;
			contributingNodesLength = -1;
		} else {
			Set<ByteArrayWrapper> nodes = new HashSet<ByteArrayWrapper>();
			int end = first.contributingNodesOffset + first.contributingNodesLength;
			for (int pos = first.contributingNodesOffset; pos < end; pos += OutputBuffer.inetSocketAddressLen) {
				nodes.add(new ByteArrayWrapper(first.contributingNodes, pos, OutputBuffer.inetSocketAddressLen));
			}
			end = second.contributingNodesOffset + second.contributingNodesLength;
			for (int pos = second.contributingNodesOffset; pos < end; pos += OutputBuffer.inetSocketAddressLen) {
				nodes.add(new ByteArrayWrapper(second.contributingNodes, pos, OutputBuffer.inetSocketAddressLen));
			}
			contributingNodes = new byte[nodes.size() * OutputBuffer.inetSocketAddressLen];
			contributingNodesOffset = 0;
			contributingNodesLength = contributingNodes.length;
			int pos = 0;
			for (ByteArrayWrapper baw : nodes) {
				baw.copyInto(contributingNodes, pos);
				pos += OutputBuffer.inetSocketAddressLen;
			}
		}
	}

	@Override
	public boolean sameSchemaAs(AbstractTuple<QpSchema> t) {
		return t.getSchema().relId == getSchema().relId;
	}

	@Override
	public boolean hasSchema(QpSchema schema) {
		return getSchema().relId == schema.relId;
	}

	public void getBytes(OutputBuffer out) {
		getHeaderBytes(out);
		super.getBytes(out);
	}
	
	public void getHeaderBytes(OutputBuffer out) {
		out.writeInt(phaseNo);
		if (contributingNodesLength >= 0) {
			out.writeInt(contributingNodesLength / OutputBuffer.inetSocketAddressLen);
			out.writeBytesNoLength(contributingNodes, contributingNodesOffset, contributingNodesLength);
		} else {
			out.writeInt(-1);
		}
		out.writeBytes(metadata, metadataOffset, metadataLength);		
	}
	
	public void getBytesWhileChanging(OutputBuffer out, byte[] metadata, byte[] contributingNodes, int phaseNo) {
		out.writeInt(phaseNo);
		if (contributingNodes != null && contributingNodes.length > 0) {
			out.writeInt(contributingNodes.length / OutputBuffer.inetSocketAddressLen);
			out.writeBytesNoLength(contributingNodes, 0, contributingNodes.length);
		} else {
			out.writeInt(-1);
		}
		out.writeBytes(metadata);
		super.getBytes(out);
	}

	public void getBytesWhileChangingPhase(OutputBuffer out, int phaseNo) {
		out.writeInt(phaseNo);
		if (contributingNodes != null && contributingNodesLength > 0) {
			out.writeInt(contributingNodesLength / OutputBuffer.inetSocketAddressLen);
			out.writeBytesNoLength(contributingNodes, contributingNodesOffset, contributingNodesLength);
		} else {
			out.writeInt(-1);
		}
		out.writeBytes(metadata);
		super.getBytes(out);
	}
	
	public byte[] getStoreBytes() {
		int length = super.getSerializedLength() + IntType.bytesPerInt;
		if (metadataLength > 0) {
			length += metadataLength;
		}
		byte[] retval = new byte[length];
		int pos = 0;
		if (metadataLength > 0) {
			IntType.putBytes(metadataLength, retval, pos);
			pos += IntType.bytesPerInt;
			System.arraycopy(metadata, metadataOffset, retval, pos, metadataLength);
			pos += metadataLength;
		} else {
			IntType.putBytes(-1, retval, pos);
			pos += IntType.bytesPerInt;
		}
		super.putBytes(retval, pos);
		return retval;
	}

	public void putStoreBytes(OutputBuffer out) {
		int length = super.getSerializedLength() + IntType.bytesPerInt;
		if (metadataLength > 0) {
			length += metadataLength;
		}
		out.writeInt(length);
		if (metadataLength > 0) {
			out.writeBytes(metadata, metadataOffset, metadataLength);
		} else {
			out.writeBytes(null);
		}
		super.getBytesNoLength(out);
	}

	public static <M> QpTuple<M> fromStoreBytes(QpSchema schema, InputBuffer in) {
		byte[] data = in.readBytesWithoutCopying();
		return fromStoreBytes(schema, data, in.lastReadOffset, in.lastReadLength, null, -1);
	}

	public static <M> QpTuple<M> fromStoreBytes(QpSchema schema, byte[] bytes) {
		return fromStoreBytes(schema, bytes,0,bytes.length, null, 0);
	}

	public static <M> QpTuple<M> fromStoreBytes(QpSchema schema, byte[] bytes, int phaseNo) {
		return fromStoreBytes(schema, bytes,0,bytes.length, null, phaseNo);
	}

	public static <M> QpTuple<M> fromStoreBytes(QpSchema schema, byte[] bytes, byte[] contributingNodes, int phaseNo) {
		return fromStoreBytes(schema, bytes, 0, bytes.length, contributingNodes, phaseNo);
	}

	public static <M> QpTuple<M> fromStoreBytes(QpSchema schema, byte[] bytes, int offset, int length, byte[] contributingNodes, int phaseNo) {
		int pos = 0;
		byte[] contributingNodeBytes;
		int contributingNodesOffset, contributingNodesLength;
		if (contributingNodes != null) {
			contributingNodeBytes = contributingNodes;
			contributingNodesOffset = 0;
			contributingNodesLength = contributingNodeBytes.length;
		} else {
			contributingNodeBytes = null;
			contributingNodesOffset = -1;
			contributingNodesLength = -1;
		}
		int metadataLength = IntType.getValFromBytes(bytes, pos + offset);
		pos += IntType.bytesPerInt;
		int metadataOffset;
		byte[] metadata;
		if (metadataLength >= 0) {
			metadataOffset = pos + offset;
			metadata = bytes;
			pos += metadataLength;
		} else {
			metadataLength = -1;
			metadataOffset = -1;
			metadata = null;
		}
		return new QpTuple<M>(schema, phaseNo, contributingNodeBytes, contributingNodesOffset, contributingNodesLength,
				metadata, metadataOffset, metadataLength, bytes, pos + offset, length - pos);
	}

	private QpTuple(QpSchema schema, int phaseNo, byte[] contributingNodes, int contributingNodesOffset, int contributingNodesLength,
			byte[] metadata, int metadataOffset, int metadataLength,
			byte[] abstractTuple, int abstractTupleOffset, int abstractTupleLength) {
		super(schema, false, abstractTuple, abstractTupleOffset, abstractTupleLength);
		this.phaseNo = phaseNo;
		this.contributingNodes = contributingNodes;
		this.contributingNodesOffset = contributingNodesOffset;
		this.contributingNodesLength = contributingNodesLength;
		this.metadata = metadata;
		this.metadataOffset = metadataOffset;
		this.metadataLength = metadataLength;
	}

	/**
	 * Create a copy of this QpTuple which is identical but does not refer to
	 * any external data
	 * 
	 * @return			The copy of the tuple
	 */
	public QpTuple<M> createSelfContainedCopy() {
		int abstractTupleLength = this.getSerializedLength();
		int dataLength = abstractTupleLength;
		int contributingNodesOffset = -1;
		if (contributingNodesLength >= 0) {
			contributingNodesOffset = dataLength;
			dataLength += contributingNodesLength;
		}
		int metadataOffset = -1;
		if (metadataLength >= 0) {
			metadataOffset = dataLength;
			dataLength += metadataLength;
		}
		byte[] data = new byte[dataLength];
		super.putBytes(data, 0);
		if (contributingNodesLength > 0) {
			System.arraycopy(contributingNodes, this.contributingNodesOffset, data, contributingNodesOffset, contributingNodesLength);
		}
		if (metadataLength >= 0) {
			System.arraycopy(metadata, this.metadataOffset, data, metadataOffset, metadataLength);
		}
		return new QpTuple<M>(schema, phaseNo, data, contributingNodesOffset, contributingNodesLength,
				metadata, metadataOffset, metadataLength, data, 0 , abstractTupleLength);
	}
	
	public static <M> QpTuple<M> fromBytes(QpSchema schema, InputBuffer in) {
		int phaseNo = in.readInt();
		int numContributingNodes = in.readInt();
		byte[] contributingNodes = null;
		int contributingNodesOffset = -1, contributingNodesLength = -1, metadataOffset = -1, metadataLength = -1;
		if (numContributingNodes >= 0) {
			contributingNodes = in.readBytesWithoutCopying(numContributingNodes * OutputBuffer.inetSocketAddressLen);
			contributingNodesOffset = in.lastReadOffset;
			contributingNodesLength = numContributingNodes * OutputBuffer.inetSocketAddressLen;
		}
		byte[] metadata = in.readBytesWithoutCopying();
		if (metadata != null) {
			metadataOffset = in.lastReadOffset;
			metadataLength = in.lastReadLength;
		}
		byte[] abstractTuple = in.readBytesWithoutCopying();
		int abstractTupleOffset = in.lastReadOffset;
		int abstractTupleLength = in.lastReadLength;
		return new QpTuple<M>(schema, phaseNo, contributingNodes, contributingNodesOffset, contributingNodesLength, metadata, metadataOffset, metadataLength, 
				abstractTuple, abstractTupleOffset, abstractTupleLength); 
	}


	public M getMetadata(QpSchema.Source findSchema, MetadataFactory<M> mdf) {
		if (metadata == null) {
			return null;
		} else {
			return mdf.fromBytes(findSchema, metadata, metadataOffset, metadataLength);
		}
	}

	Id getQPid(int[] cols) {
		return Id.fromContent(schema.getBytesForId(this,cols,null));
	}

	Id getQPid(int[] cols, byte[] scratch) {
		return Id.fromContent(schema.getBytesForId(this,cols,scratch));
	}

	Id getQPid(byte[] scratch) {
		return Id.fromContent(schema.getBytesForId(this,scratch));
	}

	public Id getQPid() {
		return Id.fromContent(schema.getBytesForId(this,null));
	}

	public int compareTo(QpTuple<?> t) {
		/*
		 * Go through fields in order
		 * null precedes non-null
		 * labeled null precedes normal null
		 */
		if (getSchema().relId != t.getSchema().relId) {
			throw new IllegalArgumentException("Should only compare tuples from same relation");
		}
		final QpSchema s = getSchema();
		final int numCols = s.getNumCols();
		for (int i = 0; i < numCols; ++i) {
			if (this.isLabeledNull(i)) {
				if (t.isLabeledNull(i)) {
					int diff = this.getLabeledNull(i) - t.getLabeledNull(i);
					if (diff != 0) {
						return diff;
					}
				} else {
					return -1;
				}
			} else if (t.isLabeledNull(i)) {
				return 1;
			}
			Object o1 = this.get(i), o2 = t.get(i);
			if (o1 == null) {
				if (o2 != null) {
					return -1;
				}
			} else if (o2 == null) {
				if (o1 != null) {
					return 1;
				}
			} else {
				int diff;
				try {
					diff = s.getColType(i).compareTwo(o1, o2);
				} catch (CompareMismatch e) {
					throw new RuntimeException(e);
				}
				if (diff != 0) {
					return diff;
				}
			}
		}
		return 0;
	}


	private QpTuple(QpTuple<?> t, byte[] contributingNodes, int contributingNodesOffset, int contributingNodesLength,
			byte[] metadata, int metadataOffset, int metadataLength, int phaseNo) {
		super(t,false);
		this.contributingNodes = contributingNodes;
		this.contributingNodesOffset = contributingNodesOffset;
		this.contributingNodesLength = contributingNodesLength;
		this.metadata = metadata;
		this.metadataOffset = metadataOffset;
		this.metadataLength = metadataLength;
		this.phaseNo = phaseNo;
	}

	public QpTuple(AbstractImmutableTuple<QpSchema> t, int phaseNo, byte[] contributingNodes, byte[] metadata) {
		super(t,false);
		this.phaseNo = phaseNo;
		this.contributingNodes = contributingNodes;
		if (contributingNodes == null) {
			contributingNodesOffset = -1;
			contributingNodesLength = -1;
		} else {
			contributingNodesOffset = 0;
			contributingNodesLength = contributingNodes.length;
		}
		this.metadata = metadata;
		if (metadata == null) {
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadataOffset = 0;
			this.metadataLength = metadata.length;
		}
	}

	public QpTuple(QpSchema schema, AbstractImmutableTuple<QpSchema> t, AbstractRelation.RelationMapping rm, int phaseNo, byte[] contributingNodes, byte[] metadata) {
		super(schema, t, false, rm);
		this.phaseNo = phaseNo;
		this.contributingNodes = contributingNodes;
		if (contributingNodes == null) {
			contributingNodesOffset = -1;
			contributingNodesLength = -1;
		} else {
			contributingNodesOffset = 0;
			contributingNodesLength = contributingNodes.length;
		}
		this.metadata = metadata;
		if (metadata == null) {
			this.metadataOffset = -1;
			this.metadataLength = -1;
		} else {
			this.metadataOffset = 0;
			this.metadataLength = metadata.length;
		}
	}
	
	QpTuple<M> changeContributingNodes(InetSocketAddress[] contributingNodes) {
		byte[] nodes;
		int nodesOffset, nodesLength;
		if (contributingNodes == null) {
			nodes = null;
			nodesOffset = -1;
			nodesLength = -1;
		} else {
			nodes = init(contributingNodes);
			nodesOffset = 0;
			nodesLength = nodes.length;
		}
		return new QpTuple<M>(this, nodes, nodesOffset, nodesLength, metadata, metadataOffset, metadataLength, phaseNo);
	}

	@SuppressWarnings("unchecked")
	<MM> QpTuple<MM> changeMetadata(MM metadata, MetadataFactory<MM> mdf) {
		if (this.isKeyTuple()) {
			throw new IllegalStateException("Key tuples do not have metadata");
		}
		if (metadata == null && this.metadata == null) {
			return (QpTuple<MM>) this;
		}
		byte[] metadataBytes;
		int metadataOffset, metadataLength;
		if (metadata == null) {
			metadataBytes = null;
			metadataOffset = -1;
			metadataLength = -1;
		} else {
			metadataBytes = mdf.toBytes(metadata);
			metadataOffset = 0;
			metadataLength = metadataBytes.length;
		}
		return new QpTuple<MM>(this, contributingNodes, contributingNodesOffset, contributingNodesLength, metadataBytes, metadataOffset, metadataLength, phaseNo);
	}

	@SuppressWarnings("unchecked")
	<MM> QpTuple<MM> changeMetadataAndContributingNodes(MM metadata, MetadataFactory<MM> mdf, byte[] contributingNodes, int phaseNo) {
		if (this.isKeyTuple()) {
			throw new IllegalStateException("Key tuples do not have metadata");
		}
		if (metadata == null && this.metadata == null && contributingNodes == null && phaseNo == this.phaseNo) {
			return (QpTuple<MM>) this;
		}
		byte[] metadataBytes;
		int metadataOffset, metadataLength;
		if (metadata == null) {
			metadataBytes = null;
			metadataOffset = -1;
			metadataLength = -1;
		} else {
			metadataBytes = mdf.toBytes(metadata);
			metadataOffset = 0;
			metadataLength = metadataBytes.length;
		}
		return new QpTuple<MM>(this, contributingNodes, 0, contributingNodes.length, metadataBytes, metadataOffset, metadataLength, phaseNo);
	}
	
	boolean hasContributingNodes() {
		return contributingNodes != null;
	}

	boolean contributes(ByteArrayWrapper node) {
		if (contributingNodes == null) {
			return false;
		}
		NODE: for (int pos = contributingNodesOffset; pos < contributingNodesOffset + contributingNodesLength; pos += OutputBuffer.inetSocketAddressLen) {
			for (int i = 0; i < OutputBuffer.inetSocketAddressLen; ++i) {
				if (node.array[node.offset + i] != contributingNodes[pos + i]) {
					continue NODE;
				}
			}
			return true;
		}
		return false;
	}

	boolean contributes(InetSocketAddress node) {
		return contributes(new ByteArrayWrapper(OutputBuffer.getBytes(node)));
	}

	public InetSocketAddress[] getContributingNodes() {
		if (contributingNodes == null) {
			return null;
		}
		InetSocketAddress[] retval = new InetSocketAddress[contributingNodesLength / OutputBuffer.inetSocketAddressLen];
		int pos = contributingNodesOffset;
		final int end = contributingNodesOffset + contributingNodesLength;
		int index = 0;
		while (pos < end) {
			retval[index++] = InputBuffer.getInetSocketAddress(contributingNodes, pos);
			pos += OutputBuffer.inetSocketAddressLen;
		}
		return retval;
	}

	public Set<InetSocketAddress> getContributingNodesSet() {
		if (contributingNodes == null) {
			return null;
		}
		Set<InetSocketAddress> retval = new HashSet<InetSocketAddress>(contributingNodesLength / OutputBuffer.inetSocketAddressLen);
		int pos = contributingNodesOffset;
		final int end = contributingNodesOffset + contributingNodesLength;
		while (pos < end) {
			retval.add(InputBuffer.getInetSocketAddress(contributingNodes, pos));
			pos += OutputBuffer.inetSocketAddressLen;
		}
		return retval;
	}	

	public Iterator<InetSocketAddress> contributingNodesIterator() {
		if (contributingNodes == null) {
			Set<InetSocketAddress> empty = Collections.emptySet();
			return empty.iterator();
		}

		return new Iterator<InetSocketAddress>() {
			int pos = contributingNodesOffset;
			final int end = contributingNodesOffset + contributingNodesLength;

			@Override
			public boolean hasNext() {
				return pos < end;
			}

			@Override
			public InetSocketAddress next() {
				if (pos >= end) {
					throw new NoSuchElementException();
				}
				InetSocketAddress retval = InputBuffer.getInetSocketAddress(contributingNodes, pos);
				pos += OutputBuffer.inetSocketAddressLen;
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

		};
	}

	public Iterator<ByteArrayWrapper> serializedContributingNodesIterator() {
		if (contributingNodes == null) {
			Set<ByteArrayWrapper> empty = Collections.emptySet();
			return empty.iterator();
		}

		return new Iterator<ByteArrayWrapper>() {
			int pos = contributingNodesOffset;
			final int end = contributingNodesOffset + contributingNodesLength;

			@Override
			public boolean hasNext() {
				return pos < end;
			}

			@Override
			public ByteArrayWrapper next() {
				if (pos >= end) {
					throw new NoSuchElementException();
				}
				ByteArrayWrapper retval = new ByteArrayWrapper(contributingNodes, pos, OutputBuffer.inetSocketAddressLen);
				pos += OutputBuffer.inetSocketAddressLen;
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

		};
	}
	int getPhase() {
		return phaseNo;
	}

	int contributingNodesHashcode() {		
		Iterator<InetSocketAddress> it = contributingNodesIterator();
		int hashCode = 0;
		while (it.hasNext()) {
			hashCode += it.next().hashCode();
		}
		return hashCode();
	}

	boolean sameContributingNodes(QpTuple<?> t) {
		if (getNumContributingNodes() != t.getNumContributingNodes()) {
			return false;
		}

		// This exploits the fact that both lists contain no duplicates
		OUTER: for (int pos = contributingNodesOffset; pos < contributingNodesOffset + contributingNodesLength; pos += OutputBuffer.inetSocketAddressLen) {
			INNER: for (int tPos = t.contributingNodesOffset; tPos < t.contributingNodesOffset + t.contributingNodesLength; tPos += OutputBuffer.inetSocketAddressLen) {
				for (int i = 0; i < OutputBuffer.inetSocketAddressLen; ++i) {
					if (this.contributingNodes[pos + i] != t.contributingNodes[tPos + i]) {
						continue INNER;
					}
				}
				continue OUTER;
			}
		return false;
		}

		return true;
	}

	QpTuple<M> addContributingNode(InetSocketAddress node) {
		return addContributingNode(node,-1);
	}

	QpTuple<M> addContributingNode(InetSocketAddress node, int phaseNo) {
		if (node == null) {
			throw new NullPointerException();
		}
		if (contributingNodes == null || contributingNodesLength == 0) {
			byte[] nodeBytes = OutputBuffer.getBytes(node);
			return new QpTuple<M>(this, nodeBytes, 0, nodeBytes.length, metadata, metadataOffset, metadataLength, phaseNo >= 0 ? phaseNo : this.phaseNo);
		}
		if (this.contributes(node)) {
			return this;
		}

		byte[] newContributingNodes = new byte[this.contributingNodesLength + OutputBuffer.inetSocketAddressLen];
		System.arraycopy(contributingNodes, contributingNodesOffset, newContributingNodes, 0, contributingNodesLength);
		OutputBuffer.getBytes(node, newContributingNodes, contributingNodesLength);
		return new QpTuple<M>(this, newContributingNodes, 0, newContributingNodes.length, metadata, metadataOffset, metadataLength, phaseNo >= 0 ? phaseNo : this.phaseNo);
	}

	QpTuple<M> changePhase(int phaseNo) {
		return new QpTuple<M>(this, this.contributingNodes, this.contributingNodesOffset, this.contributingNodesLength,
				this.metadata, this.metadataOffset, this.metadataLength, phaseNo);
	}

	static int getQpTuplePartSerializedLength(byte[] data, int offset) {
		int numContributingNodes = IntType.getValFromBytes(data, offset +  IntType.bytesPerInt);
		if (numContributingNodes < 0) {
			numContributingNodes = 0;
		}
		int mdLength = IntType.getValFromBytes(data, offset + 2 * IntType.bytesPerInt + numContributingNodes * OutputBuffer.inetSocketAddressLen);
		if (mdLength < 0) {
			mdLength = 0;
		}
		return 3 * IntType.bytesPerInt + numContributingNodes * OutputBuffer.inetSocketAddressLen + mdLength;
	}

	static <T> void combineQpTupleParts(WriteableByteArray out, int phase, QpSchema.Source findSchema, MetadataFactory<T> mdf, byte[] tuple1, int offset1, byte[] tuple2, int offset2) {
		int numNodes1 = IntType.getValFromBytes(tuple1, offset1 + IntType.bytesPerInt);
		int numNodes2 = IntType.getValFromBytes(tuple2, offset2 + IntType.bytesPerInt);
		boolean nodesNull = numNodes1 < 0 || numNodes2 < 0;
		if (numNodes1 < 0) {
			numNodes1 = 0;
		}
		if (numNodes2 < 0) {
			numNodes2 = 0;
		}

		final int m1Length = IntType.getValFromBytes(tuple1, offset1 + 2 * IntType.bytesPerInt + numNodes1 * OutputBuffer.inetSocketAddressLen);
		final int m2Length = IntType.getValFromBytes(tuple2, offset2 + 2 * IntType.bytesPerInt + numNodes2 * OutputBuffer.inetSocketAddressLen);
		final byte[] resultMetadata;
		if (m1Length >= 0 && m2Length >= 0) {
			T m1 = mdf.fromBytes(findSchema, tuple1, offset1 + numNodes1 * OutputBuffer.inetSocketAddressLen + 3 * IntType.bytesPerInt, m1Length);
			T m2 = mdf.fromBytes(findSchema, tuple2, offset2 + numNodes2 * OutputBuffer.inetSocketAddressLen + 3 * IntType.bytesPerInt, m2Length);
			resultMetadata = mdf.toBytes(mdf.multiplyMetadata(m1, m2));
		} else {
			resultMetadata = null;
		}

		int outputSize = 3 * IntType.bytesPerInt + (resultMetadata == null ? 0 : resultMetadata.length);
		final int metadataOffset;
		final byte[] outArray;
		final int outArrayOffset;
		if (nodesNull) {
			outArrayOffset = out.getWriteableByteArrayOffset(outputSize, false);
			outArray = out.getWriteableByteArray(); 
			IntType.putBytes(-1, outArray, outArrayOffset + IntType.bytesPerInt);
			metadataOffset = 2 * IntType.bytesPerInt;
		} else if (numNodes1 + numNodes2 > 100) {
			// Build set
			Set<ByteArrayWrapper> nodes = new HashSet<ByteArrayWrapper>();
			for (int i = 0; i < numNodes1; ++i) {
				nodes.add(new ByteArrayWrapper(tuple1, offset1 + 2 * IntType.bytesPerInt + i * OutputBuffer.inetSocketAddressLen, OutputBuffer.inetSocketAddressLen));
			}
			for (int i = 0; i < numNodes2; ++i) {
				nodes.add(new ByteArrayWrapper(tuple2, offset2 + 2 * IntType.bytesPerInt + i * OutputBuffer.inetSocketAddressLen, OutputBuffer.inetSocketAddressLen));
			}

			outputSize += nodes.size() * OutputBuffer.inetSocketAddressLen;
			metadataOffset = 2 * IntType.bytesPerInt + nodes.size() * OutputBuffer.inetSocketAddressLen;
			outArrayOffset = out.getWriteableByteArrayOffset(outputSize, false);
			outArray = out.getWriteableByteArray();
			IntType.putBytes(nodes.size(), outArray, outArrayOffset + IntType.bytesPerInt);
			int count = 0;
			for (ByteArrayWrapper baw : nodes) {
				baw.copyInto(outArray, outArrayOffset + 2 * IntType.bytesPerInt + count * OutputBuffer.inetSocketAddressLen);
				++count;
			}
		} else {
			// Use repeated iterative search
			boolean add[] = new boolean[numNodes2];
			int newNumNodes = numNodes1;
			OUTER: for (int i = 0; i < numNodes2; ++i) {
				INNER: for (int j = 0; j < numNodes1; ++j) {
					for (int k = 0; k < OutputBuffer.inetSocketAddressLen; ++k) {
						if (tuple1[offset1 + 2 * IntType.bytesPerInt + j * OutputBuffer.inetSocketAddressLen + k] != 
							tuple2[offset2 + 2 * IntType.bytesPerInt + i * OutputBuffer.inetSocketAddressLen + k]) {
							// Two nodes are NOT the same
							continue INNER;
						}
					}
					// Found two nodes that are the same
					continue OUTER;
				}
			// Found node in second tuple that is not in first tuple
			add[i] = true;
			++newNumNodes;
			}
			outputSize += newNumNodes * OutputBuffer.inetSocketAddressLen;
			metadataOffset = 2 * IntType.bytesPerInt + newNumNodes * OutputBuffer.inetSocketAddressLen;
			outArrayOffset = out.getWriteableByteArrayOffset(outputSize, false);
			outArray = out.getWriteableByteArray();
			IntType.putBytes(newNumNodes, outArray, outArrayOffset + IntType.bytesPerInt);
			int pos = outArrayOffset + 2 * IntType.bytesPerInt;
			int tuple1start = offset1 + 2 * IntType.bytesPerInt;
			System.arraycopy(tuple1, tuple1start, outArray, pos, numNodes1 * OutputBuffer.inetSocketAddressLen);
			pos += numNodes1 * OutputBuffer.inetSocketAddressLen;
			for (int i = 0; i < numNodes2; ++i) {
				if (add[i]) {
					final int offset = offset2 + 2 * IntType.bytesPerInt + i * OutputBuffer.inetSocketAddressLen;
					for (int j = 0; j < OutputBuffer.inetSocketAddressLen; ++j) {
						outArray[pos++] = tuple2[offset + j];
					}
				}
			}
		}
		IntType.putBytes(phase, outArray, outArrayOffset);
		if (resultMetadata == null) {
			IntType.putBytes(-1, outArray, outArrayOffset + metadataOffset);
		} else {
			IntType.putBytes(resultMetadata.length, outArray, outArrayOffset + metadataOffset);
			System.arraycopy(resultMetadata, 0, outArray, outArrayOffset + metadataOffset + IntType.bytesPerInt, resultMetadata.length);
		}
	}

	static int getHashCode(QpSchema schema, int[] cols, byte[] tuple, boolean onlyKey, int offset) {
		int headerLength = getQpTuplePartSerializedLength(tuple, offset);
		int tupleLength = IntType.getValFromBytes(tuple, offset + headerLength);
		return schema.getHashCode(cols, tuple, onlyKey, offset + headerLength + IntType.bytesPerInt, tupleLength);
	}
	
	static void writeFromStoreBytesWhileChanging(OutputBuffer out, byte[] tuple, int offset, int length, byte[] nodeIds, byte[] metadata, int phase) {
		int metadataLength = IntType.getValFromBytes(tuple, offset);
		if (metadataLength < 0) {
			metadataLength = 0;
		}
		out.writeInt(phase);
		if (nodeIds == null) {
			out.writeInt(-1);
		} else {
			out.writeInt(nodeIds.length / OutputBuffer.inetSocketAddressLen);
			out.writeBytesNoLength(nodeIds);
		}
		out.writeBytes(metadata);
		out.writeBytes(tuple, offset + IntType.bytesPerInt + metadataLength, length - IntType.bytesPerInt - metadataLength);
	}
	
	static void writeQpTuplePartAddingNode(byte[] tuple, int offset, byte[] nodeIdToAdd, OutputBuffer out) {
		int numContributingNodes = IntType.getValFromBytes(tuple, offset + IntType.bytesPerInt);
		if (numContributingNodes < 0) {
			throw new IllegalArgumentException("Should not call this method when tuple has null contributing nodes");
		}
		boolean found = false;
		NODE: for (int i = 0; i < numContributingNodes; ++i) {
			for (int j = 0; j < OutputBuffer.inetSocketAddressLen; ++j) {
				if (nodeIdToAdd[j] != tuple[offset + 2 * IntType.bytesPerInt + i * OutputBuffer.inetSocketAddressLen + j]) {
					continue NODE;
				}
			}
			found = true;
			break;
		}
		int metadataLength = IntType.getValFromBytes(tuple, offset + 2 * IntType.bytesPerInt + numContributingNodes * OutputBuffer.inetSocketAddressLen);
		if (found) {
			if (metadataLength < 0) {
				metadataLength = 0;
			}
			// Don't need to change anything
			int length = 3 * IntType.bytesPerInt + metadataLength + numContributingNodes * OutputBuffer.inetSocketAddressLen;
			out.writeBytesNoLength(tuple, offset, length);
		} else {
			int phase = IntType.getValFromBytes(tuple, offset);
			out.writeInt(phase);
			out.writeInt(numContributingNodes + 1);
			// Write already contributing nodes
			out.writeBytesNoLength(tuple, offset + 2 * IntType.bytesPerInt, numContributingNodes * OutputBuffer.inetSocketAddressLen);
			// Add new contributing node
			out.writeBytesNoLength(nodeIdToAdd);
			// Write metadata
			out.writeInt(metadataLength);
			if (metadataLength > 0) {
				out.writeBytesNoLength(tuple, 3 * IntType.bytesPerInt + numContributingNodes * OutputBuffer.inetSocketAddressLen, metadataLength);
			}
		}
		
	}

	static boolean contributes(byte[] tuple, int offset, ByteArraySet failedNodes) {
		int numContributingNodes = IntType.getValFromBytes(tuple, offset + IntType.bytesPerInt);
		if (numContributingNodes <= 0) {
			return false;
		}
		int pos = offset + 2 * IntType.bytesPerInt;
		for (int i = 0; i < numContributingNodes; ++i) {
			if (failedNodes.contains(tuple, pos, OutputBuffer.inetSocketAddressLen)) {
				return true;
			}
			pos += OutputBuffer.inetSocketAddressLen;
		}
		return false;
	}
	
	static int getPhase(byte[] tuple, int offset) {
		return IntType.getValFromBytes(tuple, offset);
	}

	public QpTupleKey getKeyTuple(int epoch) {
		return new QpTupleKey(this, epoch);
	}

	public int getNumContributingNodes() {
		if (this.contributingNodesLength <= 0) {
			return 0;
		} else {
			return this.contributingNodesLength / OutputBuffer.inetSocketAddressLen;
		}
	}

	public void addContributingNodesTo(Collection<InetSocketAddress> nodeIds) {
		if (contributingNodes == null) {
			throw new NullPointerException();
		}
		int pos = contributingNodesOffset;
		int end = contributingNodesOffset + contributingNodesLength;
		while (pos < end) {
			nodeIds.add(InputBuffer.getInetSocketAddress(contributingNodes, pos));
			pos += OutputBuffer.inetSocketAddressLen;
		}
	}
	
	void changeData(InputBuffer in) {
		phaseNo = in.readInt();
		int numContributingNodes = in.readInt();
		if (numContributingNodes >= 0) {
			contributingNodes = in.readBytesWithoutCopying(numContributingNodes * OutputBuffer.inetSocketAddressLen);
			contributingNodesOffset = in.lastReadOffset;
			contributingNodesLength = numContributingNodes * OutputBuffer.inetSocketAddressLen;
		} else {
			contributingNodes = null;
			contributingNodesOffset = -1;
			contributingNodesLength = -1;
		}
		metadata = in.readBytesWithoutCopying();
		if (metadata == null) {
			metadataOffset = -1;
			metadataLength = -1;
		} else {
			metadataOffset = in.lastReadOffset;
			metadataLength = in.lastReadLength;
		}
		byte[] abstractTuple = in.readBytesWithoutCopying();
		int abstractTupleOffset = in.lastReadOffset;
		int abstractTupleLength = in.lastReadLength;
		this.changeData(abstractTuple, abstractTupleOffset, abstractTupleLength);
	}
	
	void changeDataFromStoreBytes(byte[] bytes, int offset, int length, byte[] contributingNodes, int phaseNo) {
		this.phaseNo = phaseNo;
		this.contributingNodes = contributingNodes;
		if (contributingNodes == null) {
			this.contributingNodesOffset = -1;
			this.contributingNodesLength = -1;
		} else {
			this.contributingNodesOffset = 0;
			this.contributingNodesLength = contributingNodes.length;
		}
		int pos = 0;
		metadataLength = IntType.getValFromBytes(bytes, pos + offset);
		pos += IntType.bytesPerInt;
		if (metadataLength >= 0) {
			metadataOffset = pos + offset;
			metadata = bytes;
			pos += metadataLength;
		} else {
			metadataLength = -1;
			metadataOffset = -1;
			metadata = null;
		}
		this.changeData(bytes, pos + offset, length - pos);
	}
	
	public void changeDataFromStoreBytes(InputBuffer in) {
		byte[] data = in.readBytesWithoutCopying();
		changeDataFromStoreBytes(data, in.lastReadOffset, in.lastReadLength, null, -1);
	}
}
