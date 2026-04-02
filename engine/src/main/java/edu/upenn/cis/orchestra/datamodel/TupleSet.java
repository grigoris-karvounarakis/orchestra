package edu.upenn.cis.orchestra.datamodel;

import java.util.Collection;


public class TupleSet extends AbstractTupleSet<Relation,Tuple> {
	private static final long serialVersionUID = 1L;

	public TupleSet() {
	}

	public TupleSet(int initialSize) {
		super(initialSize);
	}

	public TupleSet(Collection<? extends Tuple> tuples) {
		super(tuples);
	}
	
	@Override
	protected Class<Relation> getSchemaClass() {
		return Relation.class;
	}
}
