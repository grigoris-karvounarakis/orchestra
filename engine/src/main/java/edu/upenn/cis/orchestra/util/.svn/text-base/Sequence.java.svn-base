package edu.upenn.cis.orchestra.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class Sequence implements Iterator<Integer> {
	private int prev;
	private final int last;
	
	public Sequence(int last) {
		this(0,last);
	}
	
	public Sequence(int start, int last) {
		prev = start - 1;
		this.last = last;
	}

	public boolean hasNext() {
		return (prev < last);
	}

	public Integer next() {
		if (prev < last) {
			++prev;
			return prev;
		} else {
			throw new NoSuchElementException();
		}
	}

	public void remove() {
		throw new UnsupportedOperationException();
	}

}
