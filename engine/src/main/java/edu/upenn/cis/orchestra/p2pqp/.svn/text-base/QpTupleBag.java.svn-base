package edu.upenn.cis.orchestra.p2pqp;

import java.util.Iterator;
import java.util.NoSuchElementException;

import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.JoinComparator;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation.RelationMapping;
import edu.upenn.cis.orchestra.util.ByteArraySet;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;
import edu.upenn.cis.orchestra.util.ScratchInputBuffer;
import edu.upenn.cis.orchestra.util.ScratchOutputBuffer;

public class QpTupleBag<M> implements Iterable<QpTuple<M>> {
	final MetadataFactory<M> mdf;
	public final QpSchema schema;
	final QpSchema.Source findSchema;
	private ScratchOutputBuffer out = null;
	private byte[] data = null;
	private int offset = -1, length = -1;
	private int size;
	private boolean hasSize;

	public QpTupleBag(QpSchema schema, QpSchema.Source findSchema, MetadataFactory<M> mdf) {
		this(schema,findSchema,mdf,256);
	}
	
	QpTupleBag(QpSchema schema, QpSchema.Source findSchema, MetadataFactory<M> mdf, int initialCapacity) {
		this.schema = schema;
		this.findSchema = findSchema;
		this.mdf = mdf;
		out = new ScratchOutputBuffer(initialCapacity);
		hasSize = true;
		size = 0;
	}

	public QpTupleBag(QpSchema schema, QpSchema.Source findSchema, MetadataFactory<M> mdf, InputBuffer in) {
		this.schema = schema;
		this.findSchema = findSchema;
		this.mdf = mdf;
		this.data = in.readBytesWithoutCopying();
		this.offset = in.lastReadOffset;
		this.length = in.lastReadLength;
		hasSize = false;
	}
	
	public QpTupleBag(QpSchema schema, QpSchema.Source findSchema, MetadataFactory<M> mdf, byte[] data, int offset, int length) {
		this.schema = schema;
		this.findSchema = findSchema;
		this.mdf = mdf;
		this.data = data;
		this.offset = offset;
		this.length = length;
		hasSize = false;
	}
	
	public QpTupleBag(QpSchema schema, QpSchema.Source findSchema, MetadataFactory<M> mdf, byte[] data) {
		this.schema = schema;
		this.findSchema = findSchema;
		this.mdf = mdf;
		this.data = data;
		this.offset = 0;
		this.length = data.length;
		hasSize = false;
	}
	
	@Override
	public Iterator<QpTuple<M>> iterator() {
		return new Iterator<QpTuple<M>>() {
			ScratchInputBuffer in = out == null ? new ScratchInputBuffer(data, offset, length) : out.getInputBuffer();
			@Override
			public boolean hasNext() {
				return (! in.finished());
			}

			@Override
			public QpTuple<M> next() {
				if (in.finished()) {
					throw new NoSuchElementException();
				}
				return QpTuple.fromBytes(schema, in);
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
	
	public Iterator<QpTuple<M>> recyclingIterator() {
		return new Iterator<QpTuple<M>>() {
			ScratchInputBuffer in = out == null ? new ScratchInputBuffer(data, offset, length) : out.getInputBuffer();
			QpTuple<M> t = null;
			@Override
			public boolean hasNext() {
				return (! in.finished());
			}

			@Override
			public QpTuple<M> next() {
				if (in.finished()) {
					throw new NoSuchElementException();
				}
				if (t == null) {
					t = QpTuple.fromBytes(schema, in);					
				} else {
					t.changeData(in);
				}
				return t;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
	
	Iterator<ByteArrayWrapper> serializedIterator() {
		return new Iterator<ByteArrayWrapper>() {
			final PositionCounter c = getPositionCounter();

			@Override
			public boolean hasNext() {
				return c.qpTuplePos >= 0;
			}

			@Override
			public ByteArrayWrapper next() {
				if (! hasNext()) {
					throw new NoSuchElementException();
				}
				ByteArrayWrapper retval = new ByteArrayWrapper(c.source, c.qpTuplePos, length);
				c.advance();
				return retval;
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}
	
	SerializedTuplePosition getSerializedTuplePositions() {
		return new SerializedTuplePosition(this.getPositionCounter(), schema);
	}
	
	public static class SerializedTuplePosition {
		public int pos;
		public int length;
		public int abstractTuplePos;
		public int abstractTupleLength;
		final public byte[] data;
		public boolean done;
		private final PositionCounter pc;
		private final QpSchema schema;
		
		SerializedTuplePosition(PositionCounter pc, QpSchema schema) {
			this.pc = pc;
			this.pos = pc.qpTuplePos;
			this.length = pc.totalLength;
			this.data = pc.source;
			this.schema = schema;
			this.abstractTuplePos = pc.abstractTuplePos;
			this.abstractTupleLength = pc.abstractTupleLength;
			done = pc.qpTuplePos < 0;
		}
		
		void advance() {
			if (pc.qpTuplePos < 0) {
				throw new NoSuchElementException();
			}
			pc.advance();
			this.pos = pc.qpTuplePos;
			this.length = pc.totalLength;
			this.abstractTuplePos = pc.abstractTuplePos;
			this.abstractTupleLength = pc.abstractTupleLength;
			done = pc.qpTuplePos < 0;			
		}
		
		int getHashCode(int[] cols) {
			return schema.getHashCode(cols, data, false, pc.abstractTuplePos, pc.abstractTupleLength);
		}
	}
	
	private PositionCounter getPositionCounter() {
		if (out == null) {
			return new PositionCounter(data, offset, length);
		} else {
			ByteArrayWrapper baw = out.getDataNoCopy();
			return new PositionCounter(baw.array, baw.offset, baw.length);
		}		
	}
	
	private static class PositionCounter {
		int qpTuplePos;
		int totalLength;
		int qpTupleLength;
		int abstractTuplePos;
		int abstractTupleLength;
		final byte[] source;
		final int endPos;
		
		PositionCounter(byte[] data, int offset, int length) {
			this.source = data;
			this.qpTuplePos = offset;
			this.endPos = offset + length;
			length = 0;
			advance();
		}

		void advance() {
			qpTuplePos += totalLength;
			if (qpTuplePos >= endPos) {
				qpTuplePos = -1;
				totalLength = -1;
				qpTupleLength = -1;
				abstractTuplePos = -1;
				abstractTupleLength = -1;
				return;
			}
			qpTupleLength = QpTuple.getQpTuplePartSerializedLength(source, qpTuplePos);
			abstractTupleLength = IntType.getValFromBytes(source, qpTuplePos + qpTupleLength);
			abstractTuplePos = qpTuplePos + qpTupleLength + IntType.bytesPerInt;
			totalLength = qpTupleLength + abstractTupleLength + IntType.bytesPerInt;
		}
	}

	private void createOutputBuffer() {
		if (length >= 0) {
			out = new ScratchOutputBuffer(2 * length);
			out.writeBytesNoLength(data, offset, length);
		} else {
			out = new ScratchOutputBuffer(1024);
		}
		data = null;
		offset = -1;
		length = -1;
	}
	
	public void add(QpTuple<M> t) {
		if (out == null) {
			createOutputBuffer();
		}
		if (! t.getSchema().quickEquals(schema)) {
			throw new IllegalArgumentException("Tuple " + t + " is not from relation " + schema);
		}
		t.getBytes(out);
		++size;
	}
	
	public M addCombiningWithExistingMetadata(RelationMapping rm, byte[] tuple, int offset) {
		// TODO: implement
		// return the diff between the old and new metadata
		throw new UnsupportedOperationException("Need to implement");
	}
	
	void addFromStoreBytesWhileChanging(byte[] tuple, byte[] metadata, byte[] contributingNodes, int phaseNo) {
		if (out == null) {
			createOutputBuffer();
		}
		QpTuple.writeFromStoreBytesWhileChanging(out, tuple, 0, tuple.length, contributingNodes, metadata, phaseNo);
		++size;
	}
	
	void addWhileChanging(QpTuple<?> t, byte[] metadata, byte[] contributingNodes, int phaseNo) {
		if (out == null) {
			createOutputBuffer();
		}
		if (! t.getSchema().quickEquals(schema)) {
			throw new IllegalArgumentException("Tuple " + t + " is not from relation " + schema);
		}
		t.getBytesWhileChanging(out, metadata, contributingNodes, phaseNo);
		++size;
	}
	
	void addWhileChangingPhase(QpTuple<M> t, int phaseNo) {
		if (out == null) {
			createOutputBuffer();
		}
		if (! t.getSchema().quickEquals(schema)) {
			throw new IllegalArgumentException("Tuple " + t + " is not from relation " + schema);
		}
		t.getBytesWhileChangingPhase(out, phaseNo);		
		++size;
	}
	
	void add(byte[] tuple, int offset, int length) {
		if (out == null) {
			createOutputBuffer();
		}
		out.writeBytesNoLength(tuple, offset, length);
		++size;
	}

	void addAndApplyMapping(RelationMapping rm, QpTuple<M> t, byte[][] otherFields) {
		if (! rm.validForOutputSchema(this.schema)) {
			throw new IllegalArgumentException("Mapping must have bag schema as output schema");
		}
		if (out == null) {
			createOutputBuffer();
		}
		t.getHeaderBytes(out);
		t.applyMapping(rm, out, false, otherFields);
	}
	
	void addAndApplyMapping(RelationMapping rm, byte[] tuple, int offset) {
		if (! rm.validForOutputSchema(this.schema)) {
			throw new IllegalArgumentException("Mapping must have bag schema as output schema");
		}
		if (out == null) {
			createOutputBuffer();
		}
		int headerLength = QpTuple.getQpTuplePartSerializedLength(tuple, offset);
		int tupleLength = IntType.getValFromBytes(tuple, offset + headerLength);
		out.writeBytesNoLength(tuple, offset, headerLength);
		rm.createTuple(out, false, tuple, offset + headerLength + IntType.bytesPerInt, tupleLength, false);
		++size;
	}
	
	public void serialize(OutputBuffer out) {
		if (this.out == null) {
			out.writeInt(length);
			out.writeBytes(data, offset, length);
		} else {
			out.writeInt(this.out.length());
			this.out.writeContents(out);
		}
	}
	
	QpTupleBag<M> applyMapping(RelationMapping rm, QpSchema newSchema) {
		if (! rm.validForInputSchema(schema)) {
			throw new IllegalArgumentException("Relation mapping is not valid for input schema" + schema);
		}
		
		if (! rm.validForOutputSchema(newSchema)) {
			throw new IllegalArgumentException("Relation mapping is not valid for output schema" + newSchema);
		}
		
		QpTupleBag<M> retval = new QpTupleBag<M>(newSchema, this.findSchema, this.mdf);
		int size = 0;
		
		
		final PositionCounter c = getPositionCounter();
		
		while (c.qpTuplePos >= 0) {
			retval.out.writeBytesNoLength(c.source, c.qpTuplePos, c.qpTupleLength);
			rm.createTuple(retval.out, false, c.source, c.abstractTuplePos, c.abstractTupleLength, false);
			c.advance();
			++size;
		}
		retval.hasSize = true;
		this.hasSize = true;
		this.size = size;
		retval.size = size;
		return retval;
	}
	
	int joinWith(int phaseNo, QpSchema firstSchema, byte[] firstTuple, int firstTupleOffset, QpTupleBag<M> output, RelationMapping joinMapping, JoinComparator joinComparator) {
		if (! joinMapping.validForInputSchemas(firstSchema, this.schema)) {
			throw new IllegalArgumentException("Relation mapping is not valid for input schemas " + firstSchema + " and " + this.schema);
		}
		if (! joinComparator.validForInputSchemas(firstSchema, this.schema)) {
			throw new IllegalArgumentException("Join comparator is not valid for input schemas " + firstSchema + " and " + this.schema);
		}
		
		final int leftTupleQpLength = QpTuple.getQpTuplePartSerializedLength(firstTuple, firstTupleOffset);
		final int leftTupleAbstractLength = IntType.getValFromBytes(firstTuple, firstTupleOffset + leftTupleQpLength);
		final int leftTupleAbstractOffset = firstTupleOffset + leftTupleQpLength + IntType.bytesPerInt;
		
		if (output.out == null) {
			output.createOutputBuffer();
		}
		
		int numTuplesProduced = 0;
		final PositionCounter pc = getPositionCounter();
		while (pc.qpTuplePos >= 0) {
			if (joinComparator.joins(firstTuple, leftTupleAbstractOffset, leftTupleAbstractLength, false, pc.source, pc.abstractTuplePos, pc.abstractTupleLength, false)) {
				// Create joined tuple
				QpTuple.combineQpTupleParts(output.out, phaseNo, findSchema, mdf, firstTuple, firstTupleOffset, pc.source, pc.qpTuplePos);
				joinMapping.createTuple(output.out, false, firstTuple, leftTupleAbstractOffset, leftTupleAbstractLength, false,
						pc.source, pc.abstractTuplePos, pc.abstractTupleLength, false, null);
				++output.size;
				++numTuplesProduced;
			}
			pc.advance();
		}
		return numTuplesProduced;
	}
	
	public void clear() {
		createOutputBuffer();
	}

	public boolean isEmpty() {
		if (data == null) {
			return out.length() == 0;
		} else {
			return length == 0;
		}
	}
	
	public int size() {
		if (! hasSize) {
			size = 0;
			final PositionCounter pc = getPositionCounter();
			while (pc.qpTuplePos >= 0) {
				++size;
				pc.advance();
			}
			hasSize = true;
		}
		return size;
	}
	
	void addFrom(QpTupleBag<M> bag) {
		if (! bag.schema.quickEquals(schema)) {
			throw new IllegalArgumentException("Must add tuples from bag with same schema (expected " + schema.getName() + ", got " + bag.schema.getName() + ")");
		}
		if (out == null) {
			createOutputBuffer();
		}
		if (bag.data != null) {
			out.writeBytesNoLength(bag.data, bag.offset, bag.length);
		} else {
			bag.out.writeContents(out);
		}
		if (bag.hasSize) {
			this.size += bag.size;
		} else {
			this.hasSize = false;
		}
	}
	
	void addFromDroppingFailed(QpTupleBag<M> bag, ByteArraySet failedNodes) {
		if (! bag.schema.quickEquals(schema)) {
			throw new IllegalArgumentException("Must add tuples from bag with same schema (expected " + schema.getName() + ", got " + bag.schema.getName() + ")");
		}
		PositionCounter pc = bag.getPositionCounter();
		if (out == null) {
			createOutputBuffer();
		}
		while (pc.qpTuplePos >= 0) {
			if (! QpTuple.contributes(pc.source, pc.qpTuplePos, failedNodes)) {
				out.writeBytesNoLength(pc.source, pc.qpTuplePos, pc.qpTupleLength + pc.abstractTupleLength + IntType.bytesPerInt);
				++bag.size;
			}
			pc.advance();
		}
	}
	
	int length() {
		if (out == null) {
			return this.length;
		} else {
			return out.length();
		}
	}
	
	QpTupleBag<M> addContributingNode(byte[] contributingNode) {
		QpTupleBag<M> retval = new QpTupleBag<M>(this.schema, this.findSchema, this.mdf, length());
		
		PositionCounter pc = this.getPositionCounter();
		while (pc.qpTuplePos >= 0) {
			QpTuple.writeQpTuplePartAddingNode(pc.source, pc.qpTuplePos, contributingNode, retval.out);
			retval.out.writeBytes(pc.source, pc.abstractTuplePos, pc.abstractTupleLength);
			++retval.size;
			pc.advance();
		}
		
		return retval;
	}
	
	public ByteArrayWrapper getBytes() {
		if (out == null) {
			return new ByteArrayWrapper(data, offset, length);
		} else {
			return out.getDataNoCopy();
		}
	}
	
	public QpTupleBag<M> clone() {
		byte[] data;
		if (this.out == null) {
			data = new byte[this.length];
			System.arraycopy(this.data, this.offset, data, 0, this.length);
		} else {
			data = out.getData();
		}
		QpTupleBag<M> newBag = new QpTupleBag<M>(this.schema, this.findSchema, this.mdf, data, 0, data.length);
		newBag.hasSize = this.hasSize;
		newBag.size = this.size;
		return newBag;
	}
}