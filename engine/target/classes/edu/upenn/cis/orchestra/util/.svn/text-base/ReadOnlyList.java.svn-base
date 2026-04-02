package edu.upenn.cis.orchestra.util;

import java.util.Collection;
import java.util.List;
import java.util.ListIterator;

public class ReadOnlyList<T> extends ReadOnlyCollection<T> implements List<T> {
	private static final long serialVersionUID = 1L;
	private final List<T> l;
	
	private ReadOnlyList(List<T> l) {
		super(l);
		this.l = l;
	}
	
	public static <T> ReadOnlyList<T> create(List<T> l) {
		if (l == null) {
			return null;
		}
		if (l instanceof ReadOnlyList) {
			return (ReadOnlyList<T>) l;
		} else {
			return new ReadOnlyList<T>(l);
		}
	}

	public void add(int arg0, T arg1) {
		throw new UnsupportedOperationException();
	}

	public boolean addAll(int arg0, Collection<? extends T> arg1) {
		throw new UnsupportedOperationException();
	}

	public T get(int arg0) {
		return l.get(arg0);
	}

	public int indexOf(Object arg0) {
		return l.indexOf(arg0);
	}

	public int lastIndexOf(Object arg0) {
		return l.lastIndexOf(arg0);
	}

	public ListIterator<T> listIterator() {
		return new Iterator<T>(l.listIterator());
	}

	public ListIterator<T> listIterator(int arg0) {
		return new Iterator<T>(l.listIterator(arg0));
	}

	public T remove(int arg0) {
		throw new UnsupportedOperationException();
	}

	public T set(int arg0, T arg1) {
		throw new UnsupportedOperationException();
	}

	public List<T> subList(int arg0, int arg1) {
		return new ReadOnlyList<T>(l.subList(arg0, arg1));
	}

	private static class Iterator<T> implements ListIterator<T>{
		final ListIterator<T> li;
		
		Iterator(ListIterator<T> li) {
			this.li = li;
		}

		public void add(T arg0) {
			throw new UnsupportedOperationException();
		}

		public boolean hasNext() {
			return li.hasNext();
		}

		public boolean hasPrevious() {
			return li.hasPrevious();
		}

		public T next() {
			return li.next();
		}

		public int nextIndex() {
			return li.nextIndex();
		}

		public T previous() {
			return li.previous();
		}

		public int previousIndex() {
			return li.previousIndex();
		}

		public void remove() {
			throw new UnsupportedOperationException();
		}

		public void set(T arg0) {
			throw new UnsupportedOperationException();
		}
	}
	
	public boolean equals(Object o) {
		return l.equals(o);
	}
	
	public int hashCode() {
		return l.hashCode();
	}
}
