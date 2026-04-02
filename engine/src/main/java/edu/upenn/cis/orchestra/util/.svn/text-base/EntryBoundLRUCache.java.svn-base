package edu.upenn.cis.orchestra.util;

/**
 * A LRU cache that stores a maximum number of objects
 * 
 * @author netaylor
 *
 * @param <K>		The type of the key
 * @param <V>		The type of the value
 */
public class EntryBoundLRUCache<K,V> extends LRUCache<K,V> {

	public EntryBoundLRUCache(long maxSize, EvictionHandler<? super K, ? super V> eh) {
		super(maxSize, new GetSize<V>() {
			public int getSize(V anObject) {
				return 1;
			}
		}, eh);
	}

	public EntryBoundLRUCache(long maxSize) {
		this(maxSize,null);
	}

}
