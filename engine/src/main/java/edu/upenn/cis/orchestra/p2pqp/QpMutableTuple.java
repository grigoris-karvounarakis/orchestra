package edu.upenn.cis.orchestra.p2pqp;

import edu.upenn.cis.orchestra.datamodel.AbstractMutableTuple;
import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public class QpMutableTuple<M> extends AbstractMutableTuple<QpSchema> {
	private static final long serialVersionUID = 1L;

	final M metadata;
	
	public QpMutableTuple(QpSchema schema) {
		super(schema);
		metadata = null;
	}

	public QpMutableTuple(QpSchema schema, Object[] fields) throws ValueMismatchException {
		super(schema);
		for (int i = 0; i < fields.length; ++i) {
			if (fields[i] != null) {
				set(i, fields[i]);
			}
		}
		metadata = null;
	}
	
	public QpMutableTuple(QpSchema schema, M metadata) {
		super(schema);
		this.metadata = metadata;
	}
	
	public QpMutableTuple(AbstractTuple<QpSchema> t, M metadata) {
		super(t);
		this.metadata = metadata;
	}

	@Override
	public boolean sameSchemaAs(AbstractTuple<QpSchema> t) {
		return this.getSchema().relId == t.getSchema().relId;
	}
	
	@Override
	public boolean hasSchema(QpSchema schema) {
		return getSchema().relId == schema.relId;
	}
}
