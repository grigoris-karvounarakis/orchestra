package edu.upenn.cis.orchestra.util;

public class LongList {
	private long[] list;
	private int length = 0;

	
	
	public LongList() {
		list = new long[20];
	}
	
	public LongList(int initialCapacity) {
		list = new long[initialCapacity];
	}
	
	public void add(long l) {
		if (list.length == length) {
			long[] newList = new long[list.length * 2];
			System.arraycopy(list, 0, newList, 0, length);
			list = newList;
		}
		list[length++] = l;
	}

	public long[] getList() {
		long[] retval = new long[length];
		System.arraycopy(list, 0, retval, 0, length);
		return retval;
	}

	public void addFrom(LongList ll) {
		addFrom(ll, 0, ll.length);
	}
	
	public void addFrom(LongList ll, int offset, int length) {
		if (this.length + length > list.length) {
			int newCapacity = list.length * 2;
			while (newCapacity < (this.length + length)) {
				newCapacity *= 2;
			}
			long[] newList = new long[newCapacity];
			System.arraycopy(list, 0, newList, 0, this.length);
			list = newList;
		}
		for (int i = offset; i < offset + length; ++i) {
			list[this.length++] = ll.list[i];
		}
	}
	
	public void clear() {
		length = 0;
	}

	public int size() {
		return length;
	}

	public String toString() {
		StringBuilder sb = new StringBuilder("[");
		if (length > 0) {
			sb.append(list[0]);
		}

		for (int i =  1; i < length; ++i) {
			sb.append(',');
			sb.append(list[i]);
		}

		sb.append("]");
		return sb.toString();
	}
	
	public boolean equals(Object o) {
		if (o == null || o.getClass() != LongList.class) {
			return false;
		}
		LongList ll = (LongList) o;
		if (ll.length != length) {
			return false;
		}
		
		for (int i = 0; i < length; ++i) {
			if (list[i] != ll.list[i]) {
				return false;
			}
		}
		
		return true;
	}
	
	public int hashCode() {
		int retval = 0;
		for (int i = 0; i < length; ++i) {
			retval = 37 * retval + (int) list[i];
		}
		return retval;
	}
	
	public long get(int index) {
		return list[index];
	}
}