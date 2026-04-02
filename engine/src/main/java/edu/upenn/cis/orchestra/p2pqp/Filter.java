package edu.upenn.cis.orchestra.p2pqp;

import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractTuple;
import edu.upenn.cis.orchestra.datamodel.exceptions.ValueMismatchException;

public interface Filter<T extends AbstractTuple<?>> {
	public boolean eval(T t) throws FilterException;

	public static class FilterException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		public FilterException(String what) {
			super(what);
		}
		public FilterException(String what, Throwable why) {
			super(what,why);
		}
	}	
	public Set<Integer> getColumns();
}

interface TupleSetFilter extends Filter<QpTuple<?>> {
	public void add(QpTuple<?> t);
	public void changeRelation(QpSchema relation, int[] columns) throws ValueMismatchException;
	public QpSchema getRelation();
}