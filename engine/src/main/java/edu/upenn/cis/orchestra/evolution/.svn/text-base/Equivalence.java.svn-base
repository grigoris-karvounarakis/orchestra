package edu.upenn.cis.orchestra.evolution;

import java.util.HashMap;
import java.util.HashSet;

public class Equivalence<T> extends HashMap<T,HashSet<T>> {
	private static final long serialVersionUID = 1L;

	public void equate(T t1, T t2) {
		HashSet<T> s1 = this.get(t1);
		HashSet<T> s2 = this.get(t2);
		assert s1 != null;
		assert s2 != null;
		s1.addAll(s2);
		for (T t : this.keySet()) {
			HashSet<T> s = this.get(t);
			if (s == s2) {
				this.put(t, s1);
			}
		}
	}
	
	public void addToUniverse(T t) {
		HashSet<T> set = new HashSet<T>();
		set.add(t);
		this.put(t,set);
	}
}
