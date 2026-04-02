package edu.upenn.cis.orchestra.p2pqp;


import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.SortedMap;
import java.util.TreeMap;

import edu.upenn.cis.orchestra.datamodel.IntType;

class SerializedTupleSet implements Iterable<SerializedTupleSet.SerializedKeyTuples> {
	public static class SerializedKeyTuples {
		// The DHT id of all of the included tuples
		final Id id;
		// The tuples for this DHT id
		final Collection<QpTupleKey> tuples;
	
		SerializedKeyTuples(Id id, Collection<QpTupleKey> tuples) {
			this.id = id;
			this.tuples = tuples;
		}
	}
	
	final QpSchema schema;

	/**
	 * Add a key tuple to the set
	 * 
	 * @param key		The tuple
	 * @return			<code>true</code> if the tuple was added to the set, <code>false</code> if it was already
	 * 					present from either a current or previous epoch
	 */
	public boolean add(QpTupleKey key) {
		Integer existing = map.put(new TreeKey(key.getQPid(scratch), key), key.epoch);
		return (existing == null);
	}

	/**
	 * Add a key tuple to the set
	 * 
	 * @param id		The tuple's DHT ID
	 * @param key		The tuple
	 * @return			<code>true</code> if the tuple was added to the set, <code>false</code> if it was already
	 * 					present from either a current or previous epoch
	 */
	public boolean add(Id id, QpTupleKey key) {
		Integer existing = map.put(new TreeKey(id, key), key.epoch);
		return (existing == null);
	}
	
	/**
	 * Determine if the set contains a tuple with the specified key and epoch
	 * 
	 * @param t				The key
	 * @return		<code>true</code> if it does, <code>false</code> if it does not
	 */
	public boolean containsAtSuppliedEpoch(QpTupleKey t) {
		return this.getEpochForKey(t) == t.epoch;
	}

	/**
	 * Determine if the set contains a tuple with the specified key at any epoch
	 * 
	 * @param t				The key
	 * @return		<code>true</code> if it does, <code>false</code> if it does not
	 */
	public boolean containsAtAnyEpoch(QpTupleKey t) {
		return this.getEpochForKey(t) != Integer.MIN_VALUE;
	}
	
	/**
	 * Get the stored epoch for a particular tuple
	 * 
	 * @param key			The key (whose epoch is ignored)
	 * @return				The epoch stored for the key, or <code>Integer.MIN_VALUE</code> if the
	 * 						key is not present
	 */
	private int getEpochForKey(QpTupleKey key) {
		Integer epoch = map.get(new TreeKey(key.getQPid(scratch), key));
		if (epoch == null) {
			return Integer.MIN_VALUE;
		} else {
			return epoch;
		}
	}
	

	/**
	 * Remove this key (from any epoch) from the set
	 * 
	 * @param key			The key to remove
	 * @return				<code>true</code> if a tuple with the same key from any epoch was found
	 * 						and removed, <code>false</code> if no such tuple was found
	 */
	public boolean removeKey(QpTupleKey key) {
		Integer existing = map.remove(new TreeKey(key.getQPid(scratch), key));
		return (existing != null);
	}

	/**
	 * Remove this key (from any epoch) from the set
	 * 
	 * @param id			The key's DHT ID
	 * @param key			The key to remove
	 * @return				<code>true</code> if a tuple with the same key from any epoch was found
	 * 						and removed, <code>false</code> if no such tuple was found
	 */
	public boolean removeKey(Id id, QpTupleKey key) {
		Integer existing = map.remove(new TreeKey(id, key));
		return (existing != null);
	}

	public boolean isEmpty() {
		return map.isEmpty();
	}

	public int size() {
		return map.size();
	}
	
	void clear() {
		map.clear();
	}
	
	private static class TreeKey implements Comparable<TreeKey> {
		final Id id;
		final QpTupleKey key;
		
		TreeKey(Id id, QpTupleKey key) {
			this.id = id;
			this.key = key.changeEpoch(0);
		}
		
		public int hashCode() {
			return id.hashCode();
		}
		
		public boolean equals(Object o) {
			TreeKey tk = (TreeKey) o;
			
			if (! id.equals(tk.id)) {
				return false;
			}
			
			return key.equals(tk.id);
		}

		public int compareTo(TreeKey tk) {
			int cmp = id.compareTo(tk.id);
			if (cmp != 0) {
				return cmp;
			}
			return key.lexicographicCompare(tk.key);
		}
	}
	
	private final SortedMap<TreeKey,Integer> map;

	private final byte[] scratch;
	
	SerializedTupleSet(QpSchema schema) {
		if (schema == null) {
			throw new NullPointerException();
		}
		this.schema = schema;
		map = new TreeMap<TreeKey,Integer>();
		scratch = new byte[schema.getNumHashCols() * IntType.bytesPerInt];
	}
	
	public Iterator<SerializedTupleSet.SerializedKeyTuples> iterator() {
		return new Iterator<SerializedTupleSet.SerializedKeyTuples>() {
			final Iterator<Map.Entry<TreeKey, Integer>> it = map.entrySet().iterator();
			Map.Entry<TreeKey,Integer> stored;
			{
				if (it.hasNext()) {
					stored = it.next();
				} else {
					stored = null;
				}
			}
			
			public boolean hasNext() {
				return (stored != null);
			}

			public SerializedTupleSet.SerializedKeyTuples next() {
				if (stored == null) {
					throw new NoSuchElementException();
				}
				TreeKey tk = stored.getKey();
				Id id = tk.id;
				List<QpTupleKey> forId = new ArrayList<QpTupleKey>();
				forId.add(tk.key.changeEpoch(stored.getValue()));
				stored = null;
				while (it.hasNext()) {
					stored = it.next();
					TreeKey next = stored.getKey();
					if (! next.id.equals(id)) {
						break;
					}
					forId.add(next.key.changeEpoch(stored.getValue()));
				}
				return new SerializedTupleSet.SerializedKeyTuples(id, forId);
			}

			public void remove() {
				throw new UnsupportedOperationException();
			}
			
		};
	}	

}
