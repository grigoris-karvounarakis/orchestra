package edu.upenn.cis.orchestra.datamodel.iterators;

import java.util.ListIterator;
import java.util.NoSuchElementException;


/**
 * A wrapper to convert a ListIterator into a ResultIterator. Since
 * the ResultIterator interface does not support updates, the underlying
 * list cannot be changed through this class (unless the list elements
 * themselves are mutable).
 * 
 * @author netaylor
 *
 * @param <T>
 */
public class ListIteratorResultIterator<T> implements ResultIterator<T> {
	ListIterator<T> li;
	
	public ListIteratorResultIterator(ListIterator<T> source) {
		li = source;
	}
	
	public void close() {
		li = null;
	}

	public boolean hasNext(){
		return li.hasNext();
	}
	

	public boolean hasPrev(){
		return li.hasPrevious();
	}

	public T next() throws NoSuchElementException {
		return li.next();
	}

	public T prev() throws NoSuchElementException {
		return li.previous();
	}

}
