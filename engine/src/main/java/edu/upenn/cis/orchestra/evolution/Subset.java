package edu.upenn.cis.orchestra.evolution;

import java.util.Arrays;

public class Subset {
	private boolean[] m_bits;
	
	public Subset(int length) {
		m_bits = new boolean[length];
	}
	
	public boolean getBit(int index) {
		return m_bits[index];
	}
	
	public void setBit(int index) {
		m_bits[index] = true;
	}
	
	public void clearBit(int index) {
		m_bits[index] = false;
	}
	
	public boolean next() {
		int pos = m_bits.length-1;
		while (m_bits[pos]) {
			m_bits[pos] = false;
			pos--;
			if (pos < 0) {
				return false;
			}
		}
		m_bits[pos] = true;
		return true;
	}

	public int size() {
		int size = 0;
		for (boolean b : m_bits) {
			if (b) {
				size++;
			}
		}
		return size;
	}

	public boolean overlaps(Subset subset) {
		assert m_bits.length == subset.m_bits.length;
		for (int i = 0; i < subset.m_bits.length; i++) {
			if (m_bits[i] && subset.m_bits[i]) {
				return true;
			}
		}
		return false;
	}
	
	public String toString() {
		return Arrays.toString(m_bits);
	}
}