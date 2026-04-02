package edu.upenn.cis.orchestra.util;

import java.util.Iterator;
import java.util.List;

public class CombinedIterator<T> implements Iterator<T> {
	private final Iterator<? extends Iterator<? extends T>> its;
	private Iterator<? extends T> it;
	
	public CombinedIterator(List<? extends Iterator<? extends T>> iterators) {
		its = iterators.iterator();
		if (its.hasNext()) {
			it = its.next();
			advance();
		} else {
			it = null;
		}
	}
	
	public boolean hasNext() {
		return (it != null);
	}

	public T next() {
		T next = it.next();
		advance();
		return next;
	}

	private void advance() {
		while (! it.hasNext()) {
			if (its.hasNext()) {
				it = its.next();
			} else {
				it = null;
				break;
			}
		}
	}
	
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
