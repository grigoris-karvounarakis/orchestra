package edu.upenn.cis.orchestra.p2pqp;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import edu.upenn.cis.orchestra.datamodel.AbstractTuple;

public class FilterConjunction<T extends AbstractTuple<?>> implements Filter<T> {
	public final Set<Filter<? super T>> filters;

	public FilterConjunction(Collection<Filter<? super T>> filters) {
		this.filters = Collections.unmodifiableSet(new HashSet<Filter<? super T>>(filters));
	}
	
	
	public boolean eval(T t) throws FilterException {
		for (Filter<? super T> f : filters) {
			if (! f.eval(t)) {
				return false;
			}
		}
		return true;
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		
		return ((FilterConjunction<?>) o).filters.equals(filters);
	}

	public Set<Integer> getColumns() {
		HashSet<Integer> columns = new HashSet<Integer>();
		for (Filter<?> f : filters) {
			columns.addAll(f.getColumns());
		}
		return columns;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("FilterConjunction(");
		boolean notFirst = false;
		for (Filter<?> f : filters) {
			if (notFirst) {
				sb.append(",");
			}
			notFirst = true;
			sb.append(f);
		}
		sb.append(")");
		return sb.toString();
	}
}
