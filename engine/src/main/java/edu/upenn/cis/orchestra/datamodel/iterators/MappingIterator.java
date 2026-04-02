/**
 * 
 */
package edu.upenn.cis.orchestra.datamodel.iterators;

import java.util.NoSuchElementException;


public abstract class MappingIterator<T,U> implements ResultIterator<T> {
	final ResultIterator<? extends U> subIterator;
	
	public MappingIterator(ResultIterator<? extends U> subIterator) {
		this.subIterator = subIterator;
	}

	public void close() throws IteratorException {
		subIterator.close();
	}

	public boolean hasNext() throws IteratorException {
		return subIterator.hasNext();
	}

	public boolean hasPrev() throws IteratorException {
		return subIterator.hasPrev();
	}

	public T next() throws IteratorException, NoSuchElementException {
		return convert(subIterator.next());
	}

	public T prev() throws IteratorException, NoSuchElementException {
		return convert(subIterator.prev());
	}
	
	protected abstract T convert(U input) throws IteratorException;
}