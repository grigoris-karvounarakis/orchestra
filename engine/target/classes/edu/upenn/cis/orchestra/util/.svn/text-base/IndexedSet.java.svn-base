package edu.upenn.cis.orchestra.util;

import java.util.Collection;
import java.util.Set;

/**
 * A set of elements that is indexed by equality and
 * a piece of derived metadata. Both <code>T</code> and
 * <code>U</code> must support contents-based equality
 * and hashing.
 * 
 * @author netaylor
 *
 * @param <T>	The type of data being stored
 * @param <U>	The type of the derived metadata
 */
public interface IndexedSet<T,U> extends Iterable<T> {
	public interface GetMetadata<T,U> {
		U getMetadata(T obj);
	}

	Set<T> getElementsForMetadata(U metadata);
	
	boolean add(T e);
	boolean addAll(Collection<? extends T> c);
	void clear();
	boolean contains(T e);
	boolean containsAll(Collection<? extends T> c);
	public boolean isEmpty();
	boolean remove(T t);
	boolean removeAll(Collection<? extends T> c);
	int size();
	
	Set<T> toSet();
	Set<U> getMetadataValues();
}
