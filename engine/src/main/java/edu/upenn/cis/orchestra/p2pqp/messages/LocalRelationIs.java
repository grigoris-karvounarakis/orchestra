package edu.upenn.cis.orchestra.p2pqp.messages;

import java.net.InetSocketAddress;

import edu.upenn.cis.orchestra.p2pqp.MetadataFactory;
import edu.upenn.cis.orchestra.p2pqp.QpMessage;
import edu.upenn.cis.orchestra.p2pqp.QpSchema;
import edu.upenn.cis.orchestra.p2pqp.QpTupleBag;
import edu.upenn.cis.orchestra.p2pqp.QpMessageSerialization.SerializationException;
import edu.upenn.cis.orchestra.util.InputBuffer;
import edu.upenn.cis.orchestra.util.OutputBuffer;

public class LocalRelationIs extends QpMessage {
	private static final long serialVersionUID = 1L;
	public final int relationId;
	private final byte[] tuples;
	public final int epoch;

	public <M> LocalRelationIs(InetSocketAddress dest, QpSchema schema, QpTupleBag<M> tuples, int epoch, MetadataFactory<M> mdf) {
		super(dest);
		
		this.tuples = tuples.getBytes().getCopy();
		this.relationId = schema.relId;
		this.epoch = epoch;
	}
	
	private LocalRelationIs(InetSocketAddress dest, LocalRelationIs other) {
		super(dest);
		this.relationId = other.relationId;
		this.tuples = other.tuples;
		this.epoch = other.epoch;
	}
	
	public LocalRelationIs retarget(InetSocketAddress newDest) {
		return new LocalRelationIs(newDest, this);
	}

	public <M> QpTupleBag<M> getTuples(QpSchema.Source findSchema, MetadataFactory<M> mdf) {
		QpSchema schema = findSchema.getSchema(relationId);
		return new QpTupleBag<M>(schema, findSchema, mdf, tuples);
	}

	public LocalRelationIs(InputBuffer buf, InetSocketAddress origin) throws SerializationException {
		super(buf,origin);
		relationId = buf.readInt();
		epoch = buf.readInt();
		tuples = buf.readBytes();
	}
	
	@Override
	protected void subclassSerialize(OutputBuffer buf) {
		buf.writeInt(relationId);
		buf.writeInt(epoch);
		buf.writeBytes(tuples);
	}
}
