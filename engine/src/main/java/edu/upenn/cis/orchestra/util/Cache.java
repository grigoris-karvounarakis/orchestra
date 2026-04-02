package edu.upenn.cis.orchestra.util;

public interface Cache<K,V> {
	V probe(K key);
	void store(K key, V value);
	void clear(K key);
	void setEvictionHandler(EvictionHandler<? super K,? super V> eh);
	public interface EvictionHandler<K,V> {
		void wasEvicted(K key, V value);
	}
	void reset();
}


