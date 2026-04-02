package edu.upenn.cis.orchestra.evolution;

public class Pair<T1, T2> {
	public final T1 first;
	public final T2 second;

	public Pair(T1 first, T2 second) {
		this.first = first;
		this.second = second;
	}
			
	public int hashCode() {
		return (first == null ? 0 : first.hashCode()) + 37 * (second == null ? 0 : second.hashCode());
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Pair<?,?> p = (Pair<?,?>) o;
		if (first == null && p.first != null) {
			return false;
		} else if (first != null && (! first.equals(p.first))) {
			return false;
		}
		if (second == null) {
			return (p.second == null);
		} else {
			return second.equals(p.second);
		}
	}
	
	public String toString() {
		return "<" + first + ", " + second + ">";
	}
}
