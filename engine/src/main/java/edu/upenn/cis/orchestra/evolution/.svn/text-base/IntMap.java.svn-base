package edu.upenn.cis.orchestra.evolution;

import java.util.Arrays;

public class IntMap {
	private int[] m_array;
	private int m_distinctKeys;		// # keys with values != -1
	private int m_largestKey;		// largest i such that m_array[i] != -1 
	
	public IntMap(int initialsize) {
		m_array = new int[initialsize];
	}

	private void ensureCapacity(int capacity) {
		if (capacity > m_array.length) {
			int[] array = new int[capacity*2];
			Arrays.fill(array, -1);
			System.arraycopy(m_array, 0, array, 0, m_array.length);
			m_array = array;
		}
	}
	
	public int get(int key) {
		ensureCapacity(key+1);
		return m_array[key];
	}
	
	public void put(int key, int value) {
		assert(value != -1);
		ensureCapacity(key+1);
		if (m_array[key] != value) {
			m_largestKey = Math.max(m_largestKey, key);
			m_array[key] = value;
			m_distinctKeys++;
		}
	}
	
	public int getDistinctKeys() {
		return m_distinctKeys;
	}
	
	public int getLargestKey() {
		return m_largestKey;
	}
	
	public int[] getArray() {
		return m_array;
	}
	
	public void clear() {
		Arrays.fill(m_array, -1);
		m_distinctKeys = 0;
		m_largestKey = -1;
	}
}
