package edu.upenn.cis.orchestra.p2pqp;

import edu.upenn.cis.orchestra.datamodel.AbstractImmutableTuple;
import edu.upenn.cis.orchestra.datamodel.AbstractRelation;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.IntType;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;
import edu.upenn.cis.orchestra.util.ByteArrayWrapper;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class QpTupleKey extends AbstractImmutableTuple<QpSchema> {
	private static final long serialVersionUID = 1L;
	public final int epoch;
	
	
	public QpTupleKey(AbstractImmutableTuple<QpSchema> t, int epoch) {
		super(t, true);
		this.epoch = epoch;
	}

	public QpTupleKey(AbstractTuple<QpSchema> t, int epoch) {
		super(t, true);
		this.epoch = epoch;
	}
	
	public QpTupleKey(QpSchema schema, Object[] fields, int epoch) throws ValueMismatchException {
		super(schema,true,fields);
		this.epoch = epoch;
	}
	
	@Override
	public boolean hasSchema(QpSchema schema) {
		return schema.quickEquals(this.schema);
	}

	@Override
	public boolean sameSchemaAs(AbstractTuple<QpSchema> t) {
		return schema.quickEquals(t.getSchema());
	}

	public void getBytes(OutputBuffer out) {
		int length = this.getSerializedLength();
		out.writeInt(length);
		out.writeInt(epoch);
		this.getBytesNoLength(out);
	}
	
	private QpTupleKey(QpSchema schema, int epoch, byte[] data, int offset, int length) {
		super(schema,true,data,offset,length);
		this.epoch = epoch;
	}
	
	public QpTupleKey(QpSchema schema, byte[] data, int offset, int length) {
		super(schema, true, data, offset + IntType.bytesPerInt, length - IntType.bytesPerInt);
		this.epoch = IntType.getValFromBytes(data, offset);
	}
	
	public static QpTupleKey deserialize(QpSchema schema, InputBuffer in) {
		int length = in.readInt();
		int epoch = in.readInt();
		byte[] data = in.readBytesWithoutCopying(length - IntType.bytesPerInt);
		return new QpTupleKey(schema, epoch, data, in.lastReadOffset, length - IntType.bytesPerInt);
	}
	
	public static ByteArrayWrapper readToByteArray(InputBuffer in) {
		return in.readByteArrayWrapperWithoutCopying();
	}
	
	public static QpTupleKey fromBytes(QpSchema schema, byte[] data, int offset, int length) {
		int epoch = IntType.getValFromBytes(data, offset);
		offset += IntType.bytesPerInt;
		length -= IntType.bytesPerInt;
		return new QpTupleKey(schema, epoch, data, offset, length);
	}
	
	public int getSerializedLength() {
		return super.getSerializedLength() + IntType.bytesPerInt;
	}
	
	public void putBytes(byte[] dest, int offset) {
		IntType.putBytes(epoch, dest, offset);
		offset += IntType.bytesPerInt;
		super.putBytes(dest, offset);
	}
	
	public byte[] getBytes() {
		byte[] retval = new byte[getSerializedLength()];
		putBytes(retval,0);
		return retval;
	}
	
	byte[] getSerializedQpId() {
		return Id.getSerializedFromContent(schema.getBytesForId(this, null));
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
	
	QpTupleKey changeEpoch(int epoch) {
		return new QpTupleKey(this, epoch);
	}
	
	public boolean equals(Object o) {
		QpTupleKey k = (QpTupleKey) o;
		return k.epoch == epoch && super.equals(k);
	}
	
	public <M> QpTuple<M> getNonKeyTuple(int phaseNo, byte[] contributingNodes, byte[] metadata) {
		return new QpTuple<M>(this, phaseNo, contributingNodes, metadata);
	}
	
	public <M> QpTuple<M> getNonKeyTuple(QpSchema schema, AbstractRelation.RelationMapping rm, int phaseNo, byte[] contributingNodes, byte[] metadata) {
		return new QpTuple<M>(schema, this, rm, phaseNo, contributingNodes, metadata);
	}	
}
