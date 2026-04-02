package edu.upenn.cis.orchestra.util;

public class Triple<T, U, V> {
	private final T first;
	private final U second;
	private final V third;

	public Triple(T first, U second, V third) {
		this.first = first;
		this.second = second;
		this.third = third;
	}
	
	public T getFirst() {
		return first;
	}

	public U getSecond() {
		return second;
	}
	
	public V getThird() {
		return third;
	}
		
	public int hashCode() {
		return (first == null ? 0 : first.hashCode()) + 37 * (second == null ? 0 : second.hashCode()) + 
			1601 * (third == null ? 0 : third.hashCode());
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Triple<?,?,?> p = (Triple<?,?,?>) o;
		if (first == null && p.first != null) {
			return false;
		} else if (first != null && (! first.equals(p.first))) {
			return false;
		}
		if (second == null && p.second != null) {
			return false;
		} else if (second != null && (! second.equals(p.second))) {
			return false;
		}
		if (third == null) {
			return (p.third == null);
		} else {
			return third.equals(p.third);
		}
	}
	
	public String toString() {
		return "(" + first + "," + second + "," + third + ")";
	}
}
