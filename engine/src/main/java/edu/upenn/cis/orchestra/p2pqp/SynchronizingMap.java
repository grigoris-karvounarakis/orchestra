package edu.upenn.cis.orchestra.p2pqp;

import java.util.Iterator;
import java.util.NoSuchElementException;

import edu.upenn.cis.orchestra.util.BoolHolder;

/**
 * A mapping from QpTuples to values. QpTuple equality is checked
 * only on the specified columns.
 * 
 * If no entries are removed from the table, synchronizing on the returned value for each key
 * is sufficient to ensure correct multi-threaded behavior. Otherwise, synchronization on the
 * entire table is needed to ensure correct semantics (but not to avoid memory corruption).
 * 
 * @author netaylor
 *
 * @param <V>
 */
public class SynchronizingMap<K,V> implements Iterable<SynchronizingMap<K,V>.Entry> {
	private static final int[] capacities = {1, 19, 37, 73, 149, 307, 613, 1229, 2503, 5003, 10007, 20021, 40039, 80077,
		160001, 320009, 640007, 1280023, 2560021, 5120029, 10000019};
	private int capacityIndex, threshold;
	private final float loadFactor;
	private final SupportingOps<K,V> so;
	private Object[] table;
	private int size = 0;

	interface SupportingOps<K,V> {
		V getNewValue();
		int hash(K key);
		boolean equals(K key1, K key2);
	}

	SynchronizingMap(SupportingOps<K,V> vf) {
		this(vf, 17, 0.75f);
	}

	SynchronizingMap(SupportingOps<K,V> vf, int capacity) {
		this(vf, capacity, 0.75f);
	}

	SynchronizingMap(SupportingOps<K,V> vf, int capacity, float loadFactor) {
		this.loadFactor = loadFactor;
		this.so = vf;
		int initialNumBuckets = (int) (capacity / loadFactor);
		capacityIndex = -1;
		for (int i = 0; i < capacities.length; ++i) {
			if (capacities[i] >= initialNumBuckets) {
				capacityIndex = i;
				break;
			}
		}
		if (capacityIndex == -1) {
			capacityIndex = capacities.length - 1;
			threshold = Integer.MAX_VALUE;
		} else {
			threshold = (int) (loadFactor * capacities[capacityIndex]);
		}

		table = new Object[capacities[capacityIndex]];
	}

	public class Entry {
		public final K key;
		public final V value;
		private final int hash;
		private Entry next;

		Entry(K key, int hash) {
			this.key = key;
			this.value = so.getNewValue();
			this.hash = hash;
		}
	}

	synchronized V getOrCreate(K key) {
		return get(key, true, null);
	}
	
	synchronized V getIfExists(K key) {
		return get(key, false, null);
	}

	protected synchronized V get(K key, boolean addIfNotPresent, BoolHolder added) {
		int hash = so.hash(key);
		int bucket = hash % table.length;
		if (bucket < 0) {
			bucket += table.length;
		}
		Entry e = (Entry) table[bucket];
		Entry forKey = null;
		if (e == null) {
			if (! addIfNotPresent) {
				return null;
			}
			forKey = new Entry(key, hash);
			table[bucket] = forKey;
			++size;
			if (added != null) {
				added.val = true;
			}
		} else {
			while (forKey == null) {
				if (e.hash == hash && so.equals(key, e.key)) {
					forKey = e;
					break;
				} else if (e.next != null) {
					e = e.next;
				} else {
					break;
				}
			}
			if (forKey == null) {
				if (! addIfNotPresent) {
					return null;
				}
				if (added != null) {
					added.val = true;
				}
				forKey = new Entry(key, hash);
				e.next = forKey;
				++size;
			} else if (added != null) {
				added.val = false;
			}
		}

		resize();
		
		return forKey.value;
	}
	
	// Must be called from within a synchronized method
	private void resize() {
		if (size > threshold) {
			++capacityIndex;
			threshold = (int) (loadFactor * capacities[capacityIndex]);
			Object[] newTable = new Object[capacities[capacityIndex]];
			for (Object o : table) {
				Entry old = (Entry) o;
				while (old != null) {
					Entry next = old.next;
					old.next = null;
					int bucket = old.hash % newTable.length;
					if (bucket < 0) {
						bucket += newTable.length;
					}
					Entry inTable = (Entry) newTable[bucket];
					if (inTable == null) {
						newTable[bucket] = old;
					} else {
						while (inTable.next != null) {
							inTable = inTable.next;
						}
						inTable.next = old;
					}
					old = next;
				}
			}
			table = newTable;
		}
	}

	public synchronized boolean contains(K key) {
		return contains(key, false);
	}

	public synchronized boolean remove(K key) {
		return contains(key, true);
	}

	private synchronized boolean contains(K key, boolean remove) {
		int hash = so.hash(key);
		int bucket = hash % table.length;
		if (bucket < 0) {
			bucket += table.length;
		}
		Entry e = (Entry) table[bucket];
		if (e == null) {
			return false;
		} else {
			Entry prev = null;
			for ( ; ; ) {
				if (e.hash == hash && so.equals(key, e.key)) {
					if (remove) {
						if (prev == null) {
							table[bucket] = e.next;
						} else {
							prev.next = e.next;
						}
						--size;
					}
					return true;
				} else if (e.next != null) {
					prev = e;
					e = e.next;
				} else {
					return false;
				}
			}
		}

	}

	public Iterator<Entry> iterator() {
		return new Iterator<Entry>() {
			Entry nextEntry;
			Entry currEntry = null;
			Entry prevEntry = null;
			int currBucket, nextBucket;
			{
				int foundBucket = -1;
				for (int i = 0; i < table.length; ++i) {
					if (table[i] != null) {
						foundBucket = i;
						break;
					}
				}
				if (foundBucket < 0) {
					nextBucket = table.length;
					currBucket = table.length;
					nextEntry = null;
				} else {
					nextBucket = foundBucket;
					currBucket = foundBucket;
					nextEntry = (Entry) table[foundBucket];
				}

			}
			public boolean hasNext() {
				return nextEntry != null;
			}

			public Entry next() {
				if (nextEntry == null) {
					throw new NoSuchElementException();
				}
				if (currEntry == null) {
					// Current entry was deleted, so don't advance the prev pointer
				} else if (prevEntry != null) {
					prevEntry = prevEntry.next;
				} else {
					// We were at the first entry in a bucket at now we're at the second
					prevEntry = currEntry;
				}
				// Clear the previous entry if we're entering a new
				// bucket
				if (currBucket != nextBucket) {
					prevEntry = null;
				}

				
				currBucket = nextBucket;
				currEntry = nextEntry;
				if (nextEntry.next != null) {
					nextEntry = nextEntry.next;
				} else {
					nextEntry = null;
					++nextBucket;
					while (nextBucket < table.length) {
						if (table[nextBucket] != null) {
							nextEntry = (Entry) table[nextBucket];
							break;
						}
						++nextBucket;
					}
				}
				return currEntry;
			}

			public void remove() {
				if (currEntry == null) {
					throw new IllegalStateException();
				}
				if (prevEntry == null) {
					// Current entry is the first item in a bucket
					table[currBucket] = currEntry.next;
				} else {
					prevEntry.next = currEntry.next;
				}
				currEntry = null;
				--size;
			}
		};
	}

	synchronized void clear() {
		capacityIndex = 1;
		threshold = (int)(loadFactor * capacities[capacityIndex]);
		table = new Object[capacities[capacityIndex]];
	}
	
	public int size() {
		return size;
	}
}