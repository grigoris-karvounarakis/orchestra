package edu.upenn.cis.orchestra.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * A wrapper that takes a set and does not allow insertions or deletions to it.
 * Elements within the set can be modified if they're not immutable.
 * 
 * @author Nick
 *
 * @param <E>
 */
public class ReadOnlySet<E> extends ReadOnlyCollection<E> implements Set<E> {
	private static final long serialVersionUID = 1L;

	ReadOnlySet(Set<E> s) {
		super(s);
	}
	
	public static <E> ReadOnlySet<E> create(Collection<E> s) {
		if (s == null) {
			return null;
		}
		if (s instanceof ReadOnlySet) {
			return (ReadOnlySet<E>) s;
		} else if (s instanceof Set){
			return new ReadOnlySet<E>((Set<E>) s);
		} else {
			return new ReadOnlySet<E>(new HashSet<E>(s));
		}
	}
}
